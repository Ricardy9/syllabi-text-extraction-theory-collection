"""
reading_leaderboard.py

Generates a leaderboard of readings that appear across the most syllabi,
using literature_from_selected_syllabi.xlsx as input.
Outputs an HTML file with an interactive bar chart.
"""

import pandas as pd
import os

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE  = "literature_from_selected_syllabi.xlsx"
OUTPUT_FILE = "reading_leaderboard.html"
TOP_N       = None        # None = no limit
MIN_SYLLABI = 2           # only include readings that appear in at least this many


# ── Load data ─────────────────────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
df = pd.read_excel(os.path.join(script_dir, INPUT_FILE))

classes_col = "Class/es where listed on syllabus"
title_col   = "Title"
author_col  = "Author(s)"
year_col    = "Year"

# ── Load university lookup from metadata (if available) ───────────────────────
METADATA_FILE = "syllabi_metadata.xlsx"
_univ_map: dict = {}
_meta_path = os.path.join(script_dir, METADATA_FILE)
if os.path.exists(_meta_path):
    _meta = pd.read_excel(_meta_path)
    for _, _row in _meta.iterrows():
        _name = str(_row.get("New Syllabus Name", "")).strip()
        _univ = str(_row.get("University where this course was taught", "")).strip()
        if not _name or not _univ or _univ.lower() == "nan":
            continue
        # Key on the full new name (with university)
        _univ_map[_name] = _univ
        # Also key on the old name format (without university suffix),
        # so lookups work against literature files generated before the
        # university was added to the syllabus name.
        _suffix = f" \u2013 {_univ}"
        if _name.endswith(_suffix):
            _univ_map[_name[: -len(_suffix)]] = _univ


# ── Build syllabi analyzed list ───────────────────────────────────────────────
syllabi_list_html = ""
if os.path.exists(_meta_path):
    _meta_sorted = _meta.sort_values(
        ["Year the Course was taught", "Course Title"],
        ascending=[False, True]
    )
    for _, s in _meta_sorted.iterrows():
        course   = str(s.get("Course Title", "")).strip()
        profs    = str(s.get("Course Professors", "")).strip()
        year     = s.get("Year the Course was taught")
        term     = str(s.get("Term (Spring, Winter, etc) the Course was Taught", "")).strip()
        univ     = str(s.get("University where this course was taught", "")).strip()

        year_str = int(year) if pd.notna(year) else ""
        term_year = f"{term} {year_str}".strip() if term and term.lower() != "nan" else str(year_str)
        profs_str = profs if profs and profs.lower() != "nan" else ""
        univ_str  = univ  if univ  and univ.lower()  != "nan" else ""

        meta_parts = [x for x in [term_year, univ_str] if x]
        meta_line  = " · ".join(meta_parts)

        syllabi_list_html += f"""
        <div class="syllabus-item">
          <span class="syllabus-course">{course}</span>
          {"<span class='syllabus-profs'>" + profs_str + "</span>" if profs_str else ""}
          {"<span class='syllabus-meta'>" + meta_line + "</span>" if meta_line else ""}
        </div>"""


# ── Count syllabi per reading ─────────────────────────────────────────────────
def count_syllabi(val):
    if pd.isna(val):
        return 0
    return len([x for x in str(val).split(";") if x.strip()])

df["syllabus_count"] = df[classes_col].apply(count_syllabi)


# ── Build display label: "Author (Year) – Title" ──────────────────────────────
def _surname(author: str) -> str:
    """Extract surname from 'Last, First' or 'First Last' format."""
    author = author.strip()
    if not author:
        return ""
    if "," in author:
        return author.split(",")[0].strip()
    return author.split()[-1]

_LOWERCASE_WORDS = {
    "a", "an", "the",
    "and", "but", "or", "nor", "for", "so", "yet",
    "as", "at", "by", "in", "of", "on", "to", "up", "via",
}

def to_title_case(text: str) -> str:
    """Apply title case, keeping articles/prepositions/conjunctions lowercase
    unless they are the first or last word, or follow a colon/em-dash."""
    words = text.split()
    result = []
    force_cap = True  # capitalize first word
    for i, word in enumerate(words):
        # Strip punctuation for comparison but preserve original
        core = word.rstrip("?!.,;:")
        is_last = (i == len(words) - 1)
        if force_cap or is_last or core.lower() not in _LOWERCASE_WORDS:
            result.append(word[0].upper() + word[1:] if word else word)
        else:
            result.append(word.lower())
        # Force capitalize after colon or em-dash
        force_cap = word.endswith(":") or word.endswith("\u2013") or word.endswith("\u2014")
    return " ".join(result)


def make_label(row):
    authors = str(row[author_col]) if pd.notna(row[author_col]) else ""
    parts   = [a.strip() for a in authors.split(";") if a.strip()]
    if not parts:
        author_label = "Unknown"
    elif len(parts) == 1:
        author_label = _surname(parts[0]) or "Unknown"
    elif len(parts) == 2:
        author_label = f"{_surname(parts[0])} & {_surname(parts[1])}"
    else:
        author_label = f"{_surname(parts[0])} et al."

    year  = int(row[year_col]) if pd.notna(row[year_col]) else "n.d."
    title = to_title_case(str(row[title_col])) if pd.notna(row[title_col]) else "Untitled"

    return f"{author_label} ({year})", title


df[["short_label", "title_label"]] = df.apply(
    lambda r: pd.Series(make_label(r)), axis=1
)


# ── Filter and rank ───────────────────────────────────────────────────────────
leaderboard = (
    df[df["syllabus_count"] >= MIN_SYLLABI]
    .sort_values("syllabus_count", ascending=False)
    .pipe(lambda d: d.head(TOP_N) if TOP_N else d)
    .reset_index(drop=True)
)

if leaderboard.empty:
    print(f"No readings appear in {MIN_SYLLABI}+ syllabi. Lower MIN_SYLLABI and re-run.")
    exit()


# ── Build HTML ────────────────────────────────────────────────────────────────
max_count = int(leaderboard["syllabus_count"].max())
min_count = int(leaderboard["syllabus_count"].min())

def bar_pct(count):
    return round(count / max_count * 100, 1)

def bar_color(count):
    t = (count - min_count) / max(max_count - min_count, 1)
    # Interpolate from #93c4e0 (light blue) → #1a5f8a (dark blue)
    r = round(0x93 + t * (0x1a - 0x93))
    g = round(0xc4 + t * (0x5f - 0xc4))
    b = round(0xe0 + t * (0x8a - 0xe0))
    return f"rgb({r},{g},{b})"

import json

rows_html = ""
for rank, row in leaderboard.iterrows():
    count   = int(row["syllabus_count"])
    short   = row["short_label"]
    title   = row["title_label"]
    pct     = bar_pct(count)
    color   = bar_color(count)
    raw_val = row[classes_col]
    syllabi_names = [s.strip() for s in str(raw_val).split(";") if s.strip()] if pd.notna(raw_val) else []
    syllabi = [{"name": s, "univ": _univ_map.get(s, "")} for s in syllabi_names]
    syllabi_attr = json.dumps(syllabi)
    rows_html += f"""
        <div class="row">
          <div class="label">
            <span class="author">{short}</span>
            <span class="title">{title}</span>
          </div>
          <div class="bar-wrap" data-syllabi='{syllabi_attr}'>
            <div class="bar-clip">
              <div class="bar" style="width:{pct}%; background:{color};">
                <span class="bar-count">{count}</span>
              </div>
            </div>
          </div>
        </div>"""

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Reading Leaderboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      font-family: 'Inter', sans-serif;
      background: #f0f4f8;
      color: #1e293b;
      min-height: 100vh;
      padding: 2.5rem 1.5rem;
    }}

    .card {{
      max-width: 900px;
      margin: 0 auto;
      background: #ffffff;
      border-radius: 16px;
      box-shadow: 0 4px 24px rgba(0,0,0,0.08);
      padding: 2.5rem 2.5rem 2rem;
    }}

    header {{
      margin-bottom: 2rem;
      border-bottom: 1px solid #e2e8f0;
      padding-bottom: 1.25rem;
    }}

    header h1 {{
      font-size: 1.5rem;
      font-weight: 700;
      color: #0f172a;
      line-height: 1.3;
    }}

    header p {{
      margin-top: 0.35rem;
      font-size: 0.875rem;
      color: #64748b;
    }}

    .row {{
      display: grid;
      grid-template-columns: 1fr 55%;
      align-items: center;
      gap: 0.75rem;
      padding: 0.65rem 0;
      border-bottom: 1px solid #f1f5f9;
    }}

    .row:last-child {{ border-bottom: none; }}

    .label {{
      display: flex;
      flex-direction: column;
      gap: 0.1rem;
      overflow: hidden;
    }}

    .author {{
      font-size: 0.8rem;
      font-weight: 600;
      color: #475569;
      white-space: nowrap;
    }}

    .title {{
      font-size: 0.82rem;
      color: #334155;
      line-height: 1.35;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }}

    .bar-wrap {{
      position: relative;
    }}

    .bar-clip {{
      background: #f1f5f9;
      border-radius: 6px;
      height: 28px;
      overflow: hidden;
    }}

    .bar {{
      height: 100%;
      border-radius: 6px;
      display: flex;
      align-items: center;
      justify-content: flex-end;
      padding-right: 8px;
      min-width: 28px;
      transition: width 0.4s ease;
    }}

    .bar-count {{
      font-size: 0.75rem;
      font-weight: 700;
      color: #ffffff;
      text-shadow: 0 1px 2px rgba(0,0,0,0.25);
      white-space: nowrap;
    }}

    footer {{
      margin-top: 1.5rem;
      font-size: 0.75rem;
      color: #94a3b8;
      text-align: right;
    }}

    .tooltip {{
      display: none;
      position: absolute;
      top: calc(100% + 6px);
      left: 0;
      z-index: 100;
      background: #1e293b;
      color: #f8fafc;
      font-size: 0.78rem;
      line-height: 1.5;
      border-radius: 8px;
      padding: 0.6rem 0.85rem;
      min-width: 200px;
      width: max-content;
      max-width: none;
      box-shadow: 0 4px 16px rgba(0,0,0,0.2);
      pointer-events: none;
    }}

    .tooltip ul {{
      margin: 0;
      padding-left: 1.1rem;
    }}

    .tooltip li {{
      margin: 0.2rem 0;
      white-space: nowrap;
    }}

    .tooltip .syllabus-univ {{
      display: block;
      font-size: 0.7rem;
      color: #94a3b8;
      white-space: nowrap;
    }}

    .bar-wrap:hover .tooltip {{
      display: block;
    }}

    .syllabi-box {{
      background: #f1f5f9;
      border-radius: 10px;
      padding: 1.25rem 1.5rem;
      margin-bottom: 2rem;
    }}

    .syllabi-box h2 {{
      font-size: 0.8rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: #64748b;
      margin-bottom: 0.85rem;
    }}

    .syllabi-grid {{
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }}

    .syllabus-item {{
      display: flex;
      flex-direction: column;
      gap: 0.1rem;
    }}

    .syllabus-course {{
      font-size: 0.78rem;
      font-weight: 600;
      color: #1e293b;
      line-height: 1.3;
    }}

    .syllabus-profs {{
      font-size: 0.72rem;
      color: #475569;
    }}

    .syllabus-meta {{
      font-size: 0.7rem;
      color: #94a3b8;
    }}
  </style>
</head>
<body>
  <div class="card">
    <header>
      <h1>Readings Appearing Across the Multiple Syllabi</h1>
      <p>{len(leaderboard)} readings &nbsp;·&nbsp; appearing in {MIN_SYLLABI}+ syllabi &nbsp;·&nbsp; bar length = share of maximum ({max_count})</p>
    </header>

    <div class="syllabi-box">
      <h2>Syllabi Analyzed</h2>
      <div class="syllabi-grid">
{syllabi_list_html}
      </div>
    </div>

    <div class="leaderboard">
{rows_html}
    </div>

    <footer>Generated from {INPUT_FILE}</footer>
  </div>
  <script>
    document.querySelectorAll('.bar-wrap[data-syllabi]').forEach(wrap => {{
      const syllabi = JSON.parse(wrap.dataset.syllabi);
      if (!syllabi.length) return;
      const tip = document.createElement('div');
      tip.className = 'tooltip';
      tip.innerHTML = '<ul>' + syllabi.map(s =>
        `<li>${{s.name}}${{s.univ ? `<span class="syllabus-univ">${{s.univ}}</span>` : ''}}</li>`
      ).join('') + '</ul>';
      wrap.appendChild(tip);
    }});
  </script>
</body>
</html>"""

out_path = os.path.join(script_dir, OUTPUT_FILE)
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Saved → {out_path}")
