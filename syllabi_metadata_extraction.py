"""
syllabi_metadata_extraction.py

Extracts structured metadata from syllabi PDFs using docling for text
extraction and regex heuristics for field inference.

Required packages:
    pip install docling pandas openpyxl
"""

import re
import random
import logging
import pandas as pd
from pathlib import Path

from docling.document_converter import DocumentConverter

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SYLLABI_FOLDER    = Path(__file__).parent / "Syllabi to Draw From"
OUTPUT_XLSX       = Path(__file__).parent / "syllabi_metadata.xlsx"
OUTPUT_CSV        = Path(__file__).parent / "syllabi_metadata.csv"

DIGITIZERS        = ["Ricardy", "Lila"]

# Characters of extracted text treated as the "first page" for header-zone
# inference. Keeps bibliography entries out of university / professor inference.
HEADER_ZONE_CHARS = 2500

COLUMNS = [
    "Original name of syllabus PDF",
    "Course Title",
    "Course Professors",
    "Year the Course was taught",
    "Term (Spring, Winter, etc) the Course was Taught",
    "University where this course was taught",
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

# Single converter instance reused across all PDFs
_converter = DocumentConverter()


# ---------------------------------------------------------------------------
# Docling extraction
# ---------------------------------------------------------------------------

def extract_text(pdf_path: Path) -> tuple[str, str]:
    """
    Convert a PDF with docling and return (full_text, header_zone).

    full_text   — entire document as markdown (preserves heading structure).
    header_zone — first HEADER_ZONE_CHARS characters, used for sensitive
                  field inference so bibliography entries are never mistaken
                  for course metadata.
    """
    result    = _converter.convert(str(pdf_path))
    full_text = result.document.export_to_markdown()
    header_zone = full_text[:HEADER_ZONE_CHARS]
    return full_text, header_zone


# ---------------------------------------------------------------------------
# Plain-text inference helpers
# ---------------------------------------------------------------------------

def _is_publisher(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in PUBLISHER_VETO)


def infer_year_from_filename(filename: str) -> str:
    """Extract a 4-digit year from the filename itself."""
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
    """
    Heuristically extract a university name from the header zone.
    Rejects publisher-sounding matches.
    """
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
    """
    Infer course title from header zone.
    Prefers an explicit 'Course Title:' label, then the first markdown
    heading (## / #), then the first substantial capitalised line.
    """
    # Explicit label
    m = re.search(
        r"(?:course\s+title|course\s+name)\s*[:\-]\s*(.+)",
        header_zone, re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()

    # First markdown heading (docling preserves these)
    for line in header_zone.splitlines():
        stripped = line.lstrip("#").strip()
        if line.startswith("#") and len(stripped) > 4:
            return stripped

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
    """
    Look for explicit instructor labels followed by a name in the header zone.
    Returns a semicolon-separated string.
    Only matches when a label is present — avoids reading-list author
    false-positives.
    """
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


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def process_pdf(pdf_path: Path) -> dict:
    filename = pdf_path.name
    log.info("Processing: %s", filename)

    row = {col: "" for col in COLUMNS}
    row["Original name of syllabus PDF"]               = filename
    row["Person in charge of digitizing this syllabus"] = random.choice(DIGITIZERS)

    # Seed year/term from filename — often the most reliable source
    year = infer_year_from_filename(filename)
    term = infer_term_from_filename(filename)

    # ------------------------------------------------------------------
    # Step 1: Extract text via docling
    # ------------------------------------------------------------------
    try:
        full_text, header_zone = extract_text(pdf_path)
        log.info(
            "  Extracted %d total chars; header zone = %d chars",
            len(full_text), len(header_zone),
        )
    except Exception as exc:
        log.error("  docling failed for %s: %s", filename, exc)
        full_text = header_zone = ""

    # ------------------------------------------------------------------
    # Step 2: Infer fields from text
    # ------------------------------------------------------------------
    title   = infer_title_from_text(header_zone)
    authors = infer_professors_from_text(header_zone)

    if not year:
        year = infer_year(full_text)
    if not term:
        term = infer_term(full_text)

    uni = infer_university(header_zone)

    # ------------------------------------------------------------------
    # Step 3: Populate row
    # ------------------------------------------------------------------
    row["Course Title"]                                     = title
    row["Course Professors"]                                = authors
    row["Year the Course was taught"]                       = year
    row["Term (Spring, Winter, etc) the Course was Taught"] = term
    row["University where this course was taught"]          = uni

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
