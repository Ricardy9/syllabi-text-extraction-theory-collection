"""
syllabi_text_review.py

Extracts text from every PDF in "Syllabi to Draw From" using pdfplumber and
produces two outputs for easy review:

  text_review/<syllabus_name>.md   — raw markdown for each syllabus
  text_review/index.html           — browser viewer with left-panel navigation,
                                     rendered markdown on the right, and a link
                                     to open the original PDF

Usage:
    python syllabi_text_review.py

Requirements:
    pip install pdfplumber markdown
"""

import logging
import textwrap
from pathlib import Path

import pdfplumber

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SYLLABI_FOLDER = Path(__file__).parent / "Syllabi to Draw From"
REVIEW_FOLDER  = Path(__file__).parent / "text_review"

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def _table_to_markdown(table_data: list) -> str:
    """Render a pdfplumber table (list of lists) as a markdown table."""
    if not table_data or not table_data[0]:
        return ""
    max_cols = max(len(row) for row in table_data)
    lines = []
    for i, row in enumerate(table_data):
        cells = [(str(c or "").replace("\n", " ").strip()) for c in row]
        cells += [""] * (max_cols - len(cells))  # pad short rows
        lines.append("| " + " | ".join(cells) + " |")
        if i == 0:
            lines.append("| " + " | ".join(["---"] * max_cols) + " |")
    return "\n".join(lines)


def extract_markdown(pdf_path: Path) -> str:
    """
    Extract text and tables from a PDF using pdfplumber.

    Strategy per page:
      1. Detect tables via find_tables() and render each as a markdown table.
      2. Crop the page to the vertical strips *between* tables and extract
         text from those strips with spatial layout preserved (layout=True).
      3. Merge text strips and tables in top-to-bottom order.
    """
    all_sections: list[str] = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            if page_num > 1:
                all_sections.append(f"---\n*Page {page_num}*")

            tables      = page.find_tables()
            # Sort tables top-to-bottom so we can build strips between them
            sorted_tbls = sorted(tables, key=lambda t: t.bbox[1])

            # items: (top_y, content_string) — collected then sorted before output
            items: list[tuple[float, str]] = []

            # --- Tables ---
            for tbl in sorted_tbls:
                md = _table_to_markdown(tbl.extract())
                if md.strip():
                    items.append((tbl.bbox[1], md))

            # --- Text strips (regions between / above / below tables) ---
            prev_bottom = 0.0
            strips: list[tuple[float, float]] = []
            for tbl in sorted_tbls:
                top, bottom = tbl.bbox[1], tbl.bbox[3]
                if top > prev_bottom:
                    strips.append((prev_bottom, top))
                prev_bottom = bottom
            strips.append((prev_bottom, page.height))

            for strip_top, strip_bottom in strips:
                if strip_bottom <= strip_top + 1:   # skip slivers
                    continue
                crop = page.crop((0, strip_top, page.width, strip_bottom))
                try:
                    text = crop.extract_text(layout=True, x_tolerance=3, y_tolerance=3)
                except TypeError:
                    # pdfplumber < 0.6 doesn't have layout param
                    text = crop.extract_text(x_tolerance=3, y_tolerance=3)
                if text and text.strip():
                    items.append((strip_top, text.strip()))

            # Emit in top-to-bottom order
            items.sort(key=lambda x: x[0])
            for _, content in items:
                all_sections.append(content)

    return "\n\n".join(all_sections)


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = textwrap.dedent("""\
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <title>Syllabi Text Review</title>
      <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ display: flex; height: 100vh; font-family: system-ui, sans-serif; }}

        /* ---- sidebar ---- */
        #sidebar {{
          width: 280px; min-width: 200px; max-width: 380px;
          background: #1e293b; color: #e2e8f0;
          overflow-y: auto; padding: 1rem 0;
          display: flex; flex-direction: column;
          resize: horizontal;
        }}
        #sidebar h2 {{
          font-size: 0.8rem; text-transform: uppercase; letter-spacing: .08em;
          color: #94a3b8; padding: 0 1rem 0.75rem;
        }}
        #sidebar button {{
          width: 100%; text-align: left; background: none; border: none;
          color: #cbd5e1; padding: 0.5rem 1rem; cursor: pointer;
          font-size: 0.85rem; line-height: 1.4;
          border-left: 3px solid transparent;
        }}
        #sidebar button:hover {{ background: #334155; color: #f1f5f9; }}
        #sidebar button.active {{
          background: #0f172a; color: #38bdf8;
          border-left-color: #38bdf8;
        }}

        /* ---- main panel ---- */
        #main {{
          flex: 1; overflow-y: auto; padding: 2rem 2.5rem;
          background: #f8fafc;
        }}
        #toolbar {{
          display: flex; align-items: center; gap: 1rem;
          margin-bottom: 1.5rem; flex-wrap: wrap;
        }}
        #toolbar h1 {{ font-size: 1.1rem; color: #1e293b; flex: 1; min-width: 0; }}
        #toolbar a {{
          font-size: 0.8rem; color: #0284c7; text-decoration: none;
          padding: 0.35rem 0.75rem; border: 1px solid #bae6fd;
          border-radius: 6px; white-space: nowrap;
        }}
        #toolbar a:hover {{ background: #e0f2fe; }}

        /* ---- stats bar ---- */
        #stats {{
          font-size: 0.75rem; color: #64748b;
          margin-bottom: 1.25rem; padding: 0.5rem 0.75rem;
          background: #f1f5f9; border-radius: 6px;
          display: flex; gap: 1.5rem; flex-wrap: wrap;
        }}

        /* ---- rendered markdown ---- */
        #content {{ max-width: 860px; }}
        #content h1,#content h2,#content h3,#content h4 {{
          margin: 1.4em 0 0.4em; color: #0f172a;
        }}
        #content h1 {{ font-size: 1.5rem; }}
        #content h2 {{ font-size: 1.25rem; border-bottom: 1px solid #e2e8f0; padding-bottom: 0.2em; }}
        #content h3 {{ font-size: 1.05rem; }}
        #content p  {{ margin: 0.6em 0; line-height: 1.7; color: #334155; }}
        #content ul,#content ol {{ margin: 0.5em 0 0.5em 1.4em; }}
        #content li  {{ margin: 0.2em 0; color: #334155; line-height: 1.6; }}
        #content table {{
          border-collapse: collapse; width: 100%; margin: 1em 0; font-size: 0.9rem;
        }}
        #content th,#content td {{
          border: 1px solid #e2e8f0; padding: 0.4em 0.7em; text-align: left;
        }}
        #content th {{ background: #f1f5f9; font-weight: 600; }}
        #content code {{
          background: #f1f5f9; padding: 0.1em 0.35em; border-radius: 4px;
          font-size: 0.88em; font-family: ui-monospace, monospace;
        }}
        #content pre code {{
          display: block; padding: 0.75em; overflow-x: auto;
        }}
        #content blockquote {{
          border-left: 3px solid #cbd5e1; padding-left: 1em; color: #64748b;
          margin: 0.75em 0;
        }}
        #content hr {{ border: none; border-top: 1px solid #e2e8f0; margin: 1.5em 0; }}

        /* ---- raw text toggle ---- */
        #raw-block {{
          display: none; background: #0f172a; color: #e2e8f0;
          padding: 1rem; border-radius: 8px; margin-top: 1rem;
          font-family: ui-monospace, monospace; font-size: 0.82rem;
          white-space: pre-wrap; word-break: break-word;
          max-height: 70vh; overflow-y: auto;
        }}
        #toggle-raw {{
          font-size: 0.78rem; background: #334155; color: #cbd5e1;
          border: none; padding: 0.3rem 0.65rem; border-radius: 5px;
          cursor: pointer;
        }}
        #toggle-raw:hover {{ background: #475569; }}

        #placeholder {{ color: #94a3b8; margin-top: 4rem; text-align: center; }}
      </style>
    </head>
    <body>
      <nav id="sidebar">
        <h2>Syllabi ({count})</h2>
        {nav_buttons}
      </nav>

      <div id="main">
        <div id="toolbar">
          <h1 id="doc-title">Select a syllabus →</h1>
          <a id="pdf-link" href="#" target="_blank" style="display:none">Open original PDF ↗</a>
          <button id="toggle-raw" style="display:none" onclick="toggleRaw()">Show raw markdown</button>
        </div>
        <div id="stats" style="display:none">
          <span id="stat-chars"></span>
          <span id="stat-words"></span>
          <span id="stat-lines"></span>
        </div>
        <div id="content"><p id="placeholder">← Select a syllabus from the sidebar</p></div>
        <pre id="raw-block"></pre>
      </div>

      <script>
        const docs = {docs_json};

        let currentRaw = "";

        function selectDoc(idx) {{
          const doc = docs[idx];
          document.getElementById("doc-title").textContent = doc.name;

          const pdfLink = document.getElementById("pdf-link");
          pdfLink.href = doc.pdf_path;
          pdfLink.style.display = "";

          document.getElementById("toggle-raw").style.display = "";

          // stats
          const statsEl = document.getElementById("stats");
          statsEl.style.display = "";
          document.getElementById("stat-chars").textContent = "Characters: " + doc.chars.toLocaleString();
          document.getElementById("stat-words").textContent = "Words: " + doc.words.toLocaleString();
          document.getElementById("stat-lines").textContent = "Lines: " + doc.lines.toLocaleString();

          // render markdown
          document.getElementById("content").innerHTML = doc.html;

          // store raw
          currentRaw = doc.raw;
          const rawBlock = document.getElementById("raw-block");
          rawBlock.textContent = currentRaw;
          rawBlock.style.display = "none";
          document.getElementById("toggle-raw").textContent = "Show raw markdown";

          // highlight active button
          document.querySelectorAll("#sidebar button").forEach((b, i) => {{
            b.classList.toggle("active", i === idx);
          }});

          window.scrollTo(0, 0);
        }}

        function toggleRaw() {{
          const block = document.getElementById("raw-block");
          const btn   = document.getElementById("toggle-raw");
          if (block.style.display === "none") {{
            block.style.display = "block";
            btn.textContent = "Hide raw markdown";
            document.getElementById("content").style.display = "none";
          }} else {{
            block.style.display = "none";
            btn.textContent = "Show raw markdown";
            document.getElementById("content").style.display = "";
          }}
        }}
      </script>
    </body>
    </html>
""")


def _md_to_html_inline(md_text: str) -> str:
    """Convert markdown to HTML, falling back to <pre> if 'markdown' not installed."""
    try:
        import markdown as md_lib
        return md_lib.markdown(
            md_text,
            extensions=["tables", "fenced_code", "nl2br"],
        )
    except ImportError:
        escaped = (md_text
                   .replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;"))
        return f"<pre style='white-space:pre-wrap'>{escaped}</pre>"


def build_html(entries: list[dict]) -> str:
    """
    entries: list of dicts with keys:
        name, pdf_path (file:// URL), raw (markdown string)
    """
    import json

    docs_list = []
    for e in entries:
        raw   = e["raw"]
        words = len(raw.split())
        lines = raw.count("\n") + 1
        docs_list.append({
            "name":     e["name"],
            "pdf_path": e["pdf_path"],
            "raw":      raw,
            "html":     _md_to_html_inline(raw),
            "chars":    len(raw),
            "words":    words,
            "lines":    lines,
        })

    nav_buttons = "\n".join(
        f'<button onclick="selectDoc({i})">{e["name"]}</button>'
        for i, e in enumerate(entries)
    )

    return _HTML_TEMPLATE.format(
        count=len(entries),
        nav_buttons=nav_buttons,
        docs_json=json.dumps(docs_list, ensure_ascii=False),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    pdf_files = sorted(SYLLABI_FOLDER.glob("*.pdf"))
    if not pdf_files:
        log.error("No PDF files found in: %s", SYLLABI_FOLDER)
        return

    REVIEW_FOLDER.mkdir(exist_ok=True)
    log.info("Found %d PDF(s). Saving extracted text to: %s", len(pdf_files), REVIEW_FOLDER)

    entries = []
    for pdf_path in pdf_files:
        stem = pdf_path.stem
        log.info("Extracting: %s", pdf_path.name)
        try:
            md_text = extract_markdown(pdf_path)
        except Exception as exc:
            log.error("  pdfplumber failed for %s: %s", pdf_path.name, exc)
            md_text = f"*Extraction failed: {exc}*"

        # Save individual .md file
        md_out = REVIEW_FOLDER / f"{stem}.md"
        md_out.write_text(md_text, encoding="utf-8")
        log.info("  Saved → %s  (%d chars)", md_out.name, len(md_text))

        entries.append({
            "name":     pdf_path.name,
            "pdf_path": pdf_path.resolve().as_uri(),
            "raw":      md_text,
        })

    # Build HTML viewer
    html_out = REVIEW_FOLDER / "index.html"
    html_out.write_text(build_html(entries), encoding="utf-8")
    log.info("HTML viewer → %s", html_out)

    print("\n" + "=" * 55)
    print("  REVIEW OUTPUT")
    print("=" * 55)
    print(f"  PDFs processed : {len(pdf_files)}")
    print(f"  Markdown files : {REVIEW_FOLDER}/")
    print(f"  HTML viewer    : {html_out}")
    print("=" * 55)
    print("\nOpen the HTML viewer in your browser to review each")
    print("syllabus side-by-side with the original PDF.\n")


if __name__ == "__main__":
    main()
