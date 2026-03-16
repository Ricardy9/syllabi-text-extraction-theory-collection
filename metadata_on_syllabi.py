"""
metadata_on_syllabi.py

Extracts structured metadata from syllabi PDFs using pdfplumber for text
extraction and the Anthropic Claude API for intelligent field inference.

Required packages:
    pip install pdfplumber pandas openpyxl anthropic python-dotenv python-dotenv
"""

import os
import re
import json
import random
import logging
import pandas as pd
from pathlib import Path

from dotenv import load_dotenv
import pdfplumber
import anthropic

# Load variables from .env (in the same directory as this script) into os.environ.
# Has no effect if .env does not exist or the variable is already set in the shell.
load_dotenv(Path(__file__).parent / ".env")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# API key is read from the ANTHROPIC_API_KEY environment variable (set in .env).
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

SYLLABI_FOLDER    = Path(__file__).parent / "Syllabi to Draw From"
OUTPUT_XLSX       = Path(__file__).parent / "metadata_on_syllabi.xlsx"
OUTPUT_CSV        = Path(__file__).parent / "metadata_on_syllabi.csv"

DIGITIZERS        = ["Ricardy", "Lila"]

# Claude model used for metadata extraction
LLM_MODEL         = "claude-haiku-4-5-20251001"

# Characters of extracted text sent to the LLM as context
LLM_CONTEXT_CHARS = 3000

COLUMNS = [
    "Original name of syllabus PDF",
    "Course Title",
    "Course Professors",
    "Year the Course was taught",
    "Term (Spring, Winter, etc) the Course was Taught",
    "University where this course was taught",
    "New Syllabus Name",
    "Person in charge of digitizing this syllabus",
]

TERM_KEYWORDS = {
    "spring": "Spring",
    "summer": "Summer",
    "fall":   "Fall",
    "autumn": "Fall",
    "winter": "Winter",
}

# If any of these words appear in a university-candidate string, reject it
# as a publisher rather than a university.
PUBLISHER_VETO = {
    "press", "publisher", "publishing", "routledge", "wiley", "springer",
    "elsevier", "sage", "norton", "penguin", "blackwell", "random house",
    "taylor", "francis", "macmillan", "palgrave", "bertelsmann",
}

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# pdfplumber extraction
# ---------------------------------------------------------------------------

def extract_text(pdf_path: Path) -> tuple[str, str]:
    """
    Extract text from a PDF with pdfplumber and return (full_text, header_zone).

    full_text   — entire document text joined across all pages.
    header_zone — first LLM_CONTEXT_CHARS characters, used as context for
                  the LLM so bibliography entries are not mistaken for metadata.
    """
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
    full_text = "\n\n".join(pages)
    header_zone = full_text[:LLM_CONTEXT_CHARS]
    return full_text, header_zone


# ---------------------------------------------------------------------------
# LLM-based metadata extraction
# ---------------------------------------------------------------------------

LLM_SYSTEM_PROMPT = """\
You are a research assistant helping to catalog academic course syllabi for a
political theory and political economy collection. Your task is to extract
structured metadata from syllabus text.

Return ONLY a valid JSON object with exactly these keys:
  "course_title"  — The official name of the course (string). Do not include
                    the course number. If ambiguous, prefer the longer, more
                    descriptive title.
  "professors"    — Full name(s) of the instructor(s), semicolon-separated
                    (e.g. "Jane Smith; John Doe"). Do not include titles like
                    "Professor" or "Dr." Do not include email addresses,
                    office hours, or any other text.
  "year"          — The 4-digit calendar year the course was taught (string),
                    e.g. "2024". If not found, return "".
  "term"          — One of: "Spring", "Summer", "Fall", "Winter". If not
                    found, return "".
  "university"    — The full name of the university or institution where the
                    course was taught (string), e.g. "Harvard University".
                    Return only the institution name — no department, no city.
                    If not found, return "".

Rules:
- Return ONLY the JSON object, no markdown, no explanation.
- If a field cannot be determined from the text, return an empty string "".
- For professors, only include people who are listed as the instructor of
  record. Do not include guest speakers, book authors, or teaching assistants.
"""

def extract_metadata_with_llm(
    header_zone: str,
    filename: str,
    filename_year: str,
    filename_term: str,
) -> dict | None:
    """
    Call the Claude API to extract metadata from syllabus header text.
    Returns a dict with keys matching COLUMNS, or None on failure.
    """
    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY not set in .env or environment — skipping LLM extraction.")
        return None

    user_message = f"""\
Below is text from the first portion of a university course syllabus PDF.
The PDF filename is: "{filename}"
{f'The filename suggests the year is {filename_year}.' if filename_year else ''}
{f'The filename suggests the term is {filename_term}.' if filename_term else ''}

Extract the metadata as instructed and return a JSON object.

--- SYLLABUS TEXT ---
{header_zone}
--- END OF TEXT ---
"""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model=LLM_MODEL,
            max_tokens=512,
            system=LLM_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        raw = message.content[0].text.strip()
        # Strip markdown code fences if the model wraps the JSON
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        data = json.loads(raw)
        return {
            "Course Title":                                     data.get("course_title", "").strip(),
            "Course Professors":                                data.get("professors",    "").strip(),
            "Year the Course was taught":                       data.get("year",          "").strip(),
            "Term (Spring, Winter, etc) the Course was Taught": data.get("term",          "").strip(),
            "University where this course was taught":          data.get("university",    "").strip(),
        }
    except json.JSONDecodeError as exc:
        log.error("  LLM returned invalid JSON for %s: %s", filename, exc)
    except Exception as exc:
        log.error("  LLM call failed for %s: %s", filename, exc)
    return None


# ---------------------------------------------------------------------------
# Regex fallback helpers (used when LLM is unavailable or fails)
# ---------------------------------------------------------------------------

def _is_publisher(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in PUBLISHER_VETO)


def infer_year_from_filename(filename: str) -> str:
    m = re.search(r"\b(19[9]\d|20[0-3]\d)\b", filename)
    return m.group() if m else ""


def infer_term_from_filename(filename: str) -> str:
    lower = filename.lower()
    for kw, norm in TERM_KEYWORDS.items():
        if kw in lower:
            return norm
    return ""


def infer_year(text: str) -> str:
    m = re.search(r"\b(19[9]\d|20[0-3]\d)\b", text)
    return m.group() if m else ""


def infer_term(text: str) -> str:
    lower = text.lower()
    for kw, norm in TERM_KEYWORDS.items():
        if kw in lower:
            return norm
    return ""


def infer_university(header_zone: str) -> str:
    patterns = [
        r"University of [A-Z][A-Za-z\s\-]+",
        r"[A-Z][A-Za-z\s\-]+ University",
        r"Universit[àáâãäå][A-Za-z\s\-de]*",
        r"[A-Z][A-Za-z\s\-]+ College",
        r"[A-Z][A-Za-z\s\-]+ School of [A-Za-z\s\-]+",
        r"[A-Z][A-Za-z\s\-]+ Institute of Technology",
    ]
    for pattern in patterns:
        for m in re.finditer(pattern, header_zone):
            candidate = m.group().strip()
            wc = len(candidate.split())
            if 2 <= wc <= 8 and not _is_publisher(candidate):
                return candidate
    return ""


def infer_title_from_text(header_zone: str) -> str:
    m = re.search(
        r"(?:course\s+title|course\s+name)\s*[:\-]\s*(.+)",
        header_zone, re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()

    skip = re.compile(
        r"^(syllabus|course|professor|instructor|spring|fall|winter|summer|"
        r"office|email|phone|prereq|credit|description|required|textbook|"
        r"readings?|schedule|week\s*\d|january|february|march|april|may|june|"
        r"july|august|september|october|november|december|\d)",
        re.IGNORECASE,
    )
    for line in header_zone.splitlines():
        line = line.strip()
        if len(line) > 12 and line[0].isupper() and not skip.match(line):
            if not re.search(r"\(\d{4}\)", line):
                return line
    return ""


def infer_professors_from_text(header_zone: str) -> str:
    pattern = re.compile(
        r"(?:professor|instructor|lecturer|taught by|faculty|prof\.?|"
        r"course\s+(?:instructor|director)|instructor\s+of\s+record)"
        r"\s*[:\-]?\s*"
        r"([A-Z][A-Za-z\-]+(?:\s+[A-Z][A-Za-z\-]+){0,3})",
        re.IGNORECASE,
    )
    seen:   set[str]  = set()
    unique: list[str] = []
    noise = {"office", "hours", "email", "phone", "notes", "syllabus", "course"}
    for m in pattern.finditer(header_zone):
        name = m.group(1).strip()
        if name.lower() in noise:
            continue
        key = name.lower()
        if key not in seen:
            seen.add(key)
            unique.append(name)
    return "; ".join(unique)


def regex_fallback(header_zone: str, full_text: str, filename: str) -> dict:
    """Run all regex heuristics and return a partial metadata dict."""
    year = infer_year_from_filename(filename) or infer_year(full_text)
    term = infer_term_from_filename(filename) or infer_term(full_text)
    return {
        "Course Title":                                     infer_title_from_text(header_zone),
        "Course Professors":                                infer_professors_from_text(header_zone),
        "Year the Course was taught":                       year,
        "Term (Spring, Winter, etc) the Course was Taught": term,
        "University where this course was taught":          infer_university(header_zone),
    }


# ---------------------------------------------------------------------------
# New Syllabus Name helpers
# ---------------------------------------------------------------------------

def _last_names(professors_str: str) -> list[str]:
    """Extract last names from a semicolon-separated professors string."""
    if not professors_str.strip():
        return []
    names = [p.strip() for p in professors_str.split(";") if p.strip()]
    return [n.split()[-1] for n in names if n.split()]


def build_new_syllabus_name(title: str, professors: str, term: str, year: str, university: str = "") -> str:
    """
    Build a standardised syllabus name:
        Course Title – Prof1Last & Prof2Last – Term Year – University
    Uses whatever fields are available; returns empty string only if title is missing.
    """
    if not title:
        return ""
    lasts     = _last_names(professors)
    prof_part = " & ".join(lasts) if lasts else ""
    term_year = " ".join(filter(None, [term, year]))

    parts = [title]
    if prof_part:
        parts.append(prof_part)
    if term_year:
        parts.append(term_year)
    if university.strip():
        parts.append(university.strip())
    return " \u2013 ".join(parts)


def sanitize_for_filename(name: str) -> str:
    """Replace characters that are invalid in macOS/Windows filenames."""
    # Colon is the main offender on macOS; also strip leading/trailing spaces
    replacements = {":" : "", "/" : "-", "\\" : "-", "|" : "-", "?" : "", "*" : "", '"' : ""}
    for char, sub in replacements.items():
        name = name.replace(char, sub)
    return name.strip()


# ---------------------------------------------------------------------------
# PDF renaming
# ---------------------------------------------------------------------------

def rename_pdfs(df: pd.DataFrame) -> None:
    """
    For every row in *df* that has a non-empty 'New Syllabus Name', check
    whether the PDF in SYLLABI_FOLDER already has that name.  If not, rename
    the file.
    """
    renamed = 0
    skipped = 0
    missing = 0

    for _, row in df.iterrows():
        new_name_raw = str(row.get("New Syllabus Name", "")).strip()
        original     = str(row.get("Original name of syllabus PDF", "")).strip()

        if not new_name_raw or not original:
            skipped += 1
            continue

        new_stem     = sanitize_for_filename(new_name_raw)
        new_filename = new_stem + ".pdf"

        if new_filename == original:
            log.info("Already correctly named: %s", original)
            skipped += 1
            continue

        src = SYLLABI_FOLDER / original
        dst = SYLLABI_FOLDER / new_filename

        if not src.exists():
            log.warning("Source file not found, skipping rename: %s", original)
            missing += 1
            continue

        if dst.exists():
            log.warning("Destination already exists, skipping rename: %s → %s", original, new_filename)
            skipped += 1
            continue

        src.rename(dst)
        log.info("Renamed: %s  →  %s", original, new_filename)
        renamed += 1

    print("\n" + "=" * 50)
    print("  RENAME SUMMARY")
    print("=" * 50)
    print(f"  Renamed          : {renamed}")
    print(f"  Already correct  : {skipped}")
    print(f"  Source not found : {missing}")
    print("=" * 50 + "\n")


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def process_pdf(pdf_path: Path) -> dict:
    filename = pdf_path.name
    log.info("Processing: %s", filename)

    row = {col: "" for col in COLUMNS}
    row["Original name of syllabus PDF"]               = filename
    row["Person in charge of digitizing this syllabus"] = random.choice(DIGITIZERS)

    filename_year = infer_year_from_filename(filename)
    filename_term = infer_term_from_filename(filename)

    # ------------------------------------------------------------------
    # Step 1: Extract text via pdfplumber
    # ------------------------------------------------------------------
    try:
        full_text, header_zone = extract_text(pdf_path)
        log.info(
            "  Extracted %d total chars; context window = %d chars",
            len(full_text), len(header_zone),
        )
    except Exception as exc:
        log.error("  pdfplumber failed for %s: %s", filename, exc)
        full_text = header_zone = ""

    # ------------------------------------------------------------------
    # Step 2: LLM extraction (primary), regex fallback (secondary)
    # ------------------------------------------------------------------
    llm_result = extract_metadata_with_llm(
        header_zone, filename, filename_year, filename_term
    )

    if llm_result:
        log.info("  LLM extraction succeeded.")
        metadata = llm_result
        # If LLM left year/term blank, try seeding from filename
        if not metadata["Year the Course was taught"] and filename_year:
            metadata["Year the Course was taught"] = filename_year
        if not metadata["Term (Spring, Winter, etc) the Course was Taught"] and filename_term:
            metadata["Term (Spring, Winter, etc) the Course was Taught"] = filename_term
    else:
        log.info("  Falling back to regex heuristics.")
        metadata = regex_fallback(header_zone, full_text, filename)

    # ------------------------------------------------------------------
    # Step 3: Populate row
    # ------------------------------------------------------------------
    for key, value in metadata.items():
        row[key] = value

    # ------------------------------------------------------------------
    # Step 4: Build the standardised New Syllabus Name
    # ------------------------------------------------------------------
    row["New Syllabus Name"] = build_new_syllabus_name(
        title      = row.get("Course Title", ""),
        professors = row.get("Course Professors", ""),
        term       = row.get("Term (Spring, Winter, etc) the Course was Taught", ""),
        year       = row.get("Year the Course was taught", ""),
        university = row.get("University where this course was taught", ""),
    )

    return row


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def score_row(row: dict) -> str:
    key_fields = [
        "Course Title",
        "Course Professors",
        "Year the Course was taught",
        "Term (Spring, Winter, etc) the Course was Taught",
        "University where this course was taught",
    ]
    filled = sum(1 for f in key_fields if row.get(f, "").strip())
    if filled == len(key_fields):
        return "success"
    if filled > 0:
        return "partial"
    return "failed"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    pdf_files = sorted(SYLLABI_FOLDER.glob("*.pdf"))
    if not pdf_files:
        log.error("No PDF files found in: %s", SYLLABI_FOLDER)
        return

    log.info("Found %d PDF(s) in '%s'", len(pdf_files), SYLLABI_FOLDER)

    rows     = []
    counters = {"success": 0, "partial": 0, "failed": 0}

    for pdf_path in pdf_files:
        try:
            row = process_pdf(pdf_path)
        except Exception as exc:
            log.error("Unexpected error processing %s: %s", pdf_path.name, exc)
            row = {col: "" for col in COLUMNS}
            row["Original name of syllabus PDF"]               = pdf_path.name
            row["Person in charge of digitizing this syllabus"] = random.choice(DIGITIZERS)
        rows.append(row)
        counters[score_row(row)] += 1

    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_excel(OUTPUT_XLSX, index=False)
    df.to_csv(OUTPUT_CSV, index=False)
    log.info("Saved Excel → %s", OUTPUT_XLSX)
    log.info("Saved CSV   → %s", OUTPUT_CSV)

    rename_pdfs(df)

    total = len(pdf_files)
    print("\n" + "=" * 50)
    print("  EXTRACTION SUMMARY")
    print("=" * 50)
    print(f"  PDFs processed   : {total}")
    print(f"  Fully parsed     : {counters['success']}")
    print(f"  Partial metadata : {counters['partial']}")
    print(f"  Failed / empty   : {counters['failed']}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
