# Syllabi Text Extraction for Theory Collection

A two-script pipeline that uses [GROBID](https://github.com/kermitt2/grobid) to automatically extract structured metadata and reading-list references from course syllabus PDFs, producing clean Excel spreadsheets for a World Bank theory collection.

---

## Overview

The pipeline has two stages:

1. **`syllabi_metadata_extraction.py`** — extracts course-level metadata from each syllabus PDF (title, professor(s), university, year, term) and writes a summary spreadsheet.
2. **`syllabus_literature_extraction.py`** — extracts every bibliographic reference from the reading lists in each syllabus, deduplicates entries that appear across multiple syllabi, and writes a formatted Excel workbook with dropdown validation.

Both scripts rely on a locally running GROBID server to parse the PDFs into TEI/XML.

---

## Requirements

### Python packages
```
pip install requests pandas openpyxl lxml
```

### GROBID
A GROBID server must be running locally at `http://localhost:8070` before either script is run. See the [GROBID documentation](https://grobid.readthedocs.io/en/latest/Grobid-service/) for setup instructions.

---

## Project Structure

```
.
├── Syllabi to Draw From/          # Place input syllabus PDFs here
├── syllabi_metadata_extraction.py # Stage 1: course metadata extraction
├── syllabus_literature_extraction.py # Stage 2: reading-list extraction
├── syllabi_metadata.xlsx          # Output of Stage 1
├── syllabi_metadata.csv           # Output of Stage 1 (CSV copy)
└── literature_from_selected_syllabi.xlsx # Output of Stage 2
```

---

## Usage

### Stage 1 — Extract course metadata

```bash
python syllabi_metadata_extraction.py
```

Reads all PDFs from `Syllabi to Draw From/` and produces `syllabi_metadata.xlsx` and `syllabi_metadata.csv` with the following columns:

| Column | Description |
|--------|-------------|
| Original name of syllabus PDF | Source filename |
| Course Title | Extracted or inferred course title |
| Course Professors | Instructor name(s), semicolon-separated |
| Year the Course was taught | 4-digit year |
| Term (Spring, Winter, etc) the Course was Taught | Academic term |
| University where this course was taught | Institution name |
| Person in charge of digitizing this syllabus | Randomly assigned digitizer |

Extraction proceeds in layers:
1. GROBID `processHeaderDocument` (returns TEI/XML or BibTeX depending on version)
2. GROBID `processFulltextDocument` for body-text inference
3. Regex heuristics applied to a "header zone" (first ~2500 body chars) to avoid false positives from bibliography entries

### Stage 2 — Extract reading-list references

```bash
python syllabus_literature_extraction.py
```

Reads all PDFs from `Syllabi to Draw From/`, calls GROBID `processFulltextDocument` with citation consolidation, and produces `literature_from_selected_syllabi.xlsx`.

Readings that appear in more than one syllabus are deduplicated using token-level Jaccard similarity on normalised titles (threshold: 0.82).

The output workbook contains two sheets:
- **Literature** — one row per unique reference with columns for citation, authors, year, title, journal/publication, issue/pages, DOI link, source type, date added, and the class(es) where the reading appeared
- **_ClassList** — reference list used by Excel dropdown validation for the "Class/es" column

Source type is inferred heuristically: journal articles, books, and white papers are distinguished by the structure of the TEI `<biblStruct>` element.

---

## Notes

- Run Stage 1 before Stage 2. The literature extraction script reads `syllabi_metadata.xlsx` to build descriptive class labels (`"Course Title" Professor – Term Year`); if the file is absent, raw PDF filenames are used instead.
- Year and term are first seeded from the PDF filename (most reliable source), then from GROBID output, then from body text.
- University inference rejects publisher names (Routledge, Wiley, Springer, etc.) that GROBID sometimes returns as affiliations.
- A summary of fully parsed / partial / failed PDFs is printed to the console after each run.
