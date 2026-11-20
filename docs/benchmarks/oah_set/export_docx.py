#!/usr/bin/env python3
"""Export docs/oah_set_article.md to docs/oah_set_article.docx.

Requires python-docx and markdown-it-py. Paths are resolved relative to this file.
"""

from datetime import datetime, timezone
from pathlib import Path

from docx import Document
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT, WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.opc.constants import RELATIONSHIP_TYPE
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Mm, Pt, RGBColor
from docx.text.run import Run
from markdown_it import MarkdownIt


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "docs/oah_set_article.md"
OUTPUT = ROOT / "docs/oah_set_article.docx"

# The measured revision; links to ../src/ files point to it on GitHub.
REVISION = "534e38340c3319fef9028d20bb00f97be65df0ab"
REPO_URL = f"https://github.com/dragonflydb/dragonfly/blob/{REVISION}/"
NOTES_BOOKMARK = "BenchmarkNotes"
HEADING_STYLES = {"h1": "Title", "h2": "Heading 1"}
INK = RGBColor.from_string("203443")
ACCENT = RGBColor.from_string("087F8C")
MUTED = RGBColor.from_string("526473")

NOTES = (
    "Five tools produced the measurements, each running the measured code on one CPU: Dragonfly's "
    "unit-test benchmarks (nine samples per case); a harness that feeds the same keys to all four "
    "sets (1,890 samples: 15 scenarios, two OAHSet builds, three containers per build, and seven "
    "rounds in each of three processes, with seed 20261116); a driver that walks sets of "
    "1,000,000 members, with and without hardware counters; Dragonfly servers driven by "
    "memtier_benchmark and a probe client, nine times for each set; and perf counting hardware "
    "events while Dragonfly saves a set, five times for OAHSet 16/2 and StringSet. The data and "
    "the scripts accompany the article in the project.",
    "Companion files in docs/benchmarks/oah_set/: README.md (methodology and reproduction); "
    "raw.csv, summary.csv, and environment.json (the harness); unit_tests.csv and "
    "unit_tests.json; walk.csv, walk.json, and walk_counters.csv; whole_set_commands.csv, "
    "command_overhead.csv, and server_commands.json (the servers); save_counters.csv and "
    "save_counters.json; run.py, run_unit_tests.py, walk.py, walk.cc, walk_counters.py, "
    "server_commands.py, save_counters.py, and bm_grow.patch; plot.py; layout.mmd (source of "
    "Figure 1); and export_docx.py (generator of this document).",
    "Public code links refer to repository revision " + REVISION + ". "
    "Benchmark references within this document point to these notes because the companion "
    "files are local project artifacts.",
)


def create_document():
    document = Document()
    section = document.sections[0]
    section.page_width = Mm(210)
    section.page_height = Mm(297)
    section.top_margin = Mm(17)
    section.bottom_margin = Mm(17)
    section.left_margin = Mm(17)
    section.right_margin = Mm(17)
    section.footer_distance = Mm(8)

    normal = document.styles["Normal"]
    normal.font.name = "Arial"
    normal.font.size = Pt(11)
    normal.font.color.rgb = INK
    normal.paragraph_format.line_spacing = 1.12
    normal.paragraph_format.space_after = Pt(7)
    normal.paragraph_format.widow_control = True

    title = document.styles["Title"]
    title.font.name = "Arial"
    title.font.size = Pt(24)
    title.font.bold = True
    title.font.color.rgb = INK
    title.paragraph_format.space_after = Pt(14)
    title.paragraph_format.keep_with_next = True

    heading = document.styles["Heading 1"]
    heading.font.name = "Arial"
    heading.font.size = Pt(14)
    heading.font.bold = True
    heading.font.color.rgb = ACCENT
    heading.paragraph_format.space_before = Pt(15)
    heading.paragraph_format.space_after = Pt(6)
    heading.paragraph_format.keep_with_next = True

    caption = document.styles["Caption"]
    caption.font.name = "Arial"
    caption.font.size = Pt(9)
    caption.font.italic = True
    caption.font.color.rgb = MUTED
    caption.paragraph_format.space_after = Pt(10)

    code_style = document.styles.add_style("Article Code", WD_STYLE_TYPE.PARAGRAPH)
    code_style.base_style = normal
    code_style.font.name = "Courier New"
    code_style.font.size = Pt(8.3)
    code_style.paragraph_format.line_spacing = 1.08
    code_style.paragraph_format.space_before = Pt(5)
    code_style.paragraph_format.space_after = Pt(10)
    code_style.paragraph_format.left_indent = Mm(2)
    code_style.paragraph_format.right_indent = Mm(2)
    code_style.paragraph_format.keep_together = True

    properties = document.core_properties
    properties.title = "Every byte still matters: inside Dragonfly's OAHSet"
    properties.author = "Borys Patochkin"
    properties.subject = "OAHSet, StringSet, and std::unordered_set: memory and performance"
    properties.keywords = "OAHSet, DenseSet, StringSet, C++, memory, benchmarks"
    properties.created = datetime.now(timezone.utc)
    properties.modified = properties.created
    return document


def add_inline(paragraph, children):
    parent = paragraph._p
    bold = False
    italic = False
    in_link = False
    for token in children or []:
        if token.type == "strong_open":
            bold = True
            continue
        if token.type == "strong_close":
            bold = False
            continue
        if token.type == "em_open":
            italic = True
            continue
        if token.type == "em_close":
            italic = False
            continue
        if token.type == "link_open":
            href = token.attrGet("href")
            hyperlink = OxmlElement("w:hyperlink")
            if href.startswith(("https://", "http://")):
                relationship = paragraph.part.relate_to(
                    href, RELATIONSHIP_TYPE.HYPERLINK, is_external=True
                )
                hyperlink.set(qn("r:id"), relationship)
            elif href.startswith("../src/"):
                source_path = (SOURCE.parent / href).resolve().relative_to(ROOT).as_posix()
                relationship = paragraph.part.relate_to(
                    REPO_URL + source_path, RELATIONSHIP_TYPE.HYPERLINK, is_external=True
                )
                hyperlink.set(qn("r:id"), relationship)
            else:
                # Benchmark files are not published, so their links jump to the notes section.
                hyperlink.set(qn("w:anchor"), NOTES_BOOKMARK)
            paragraph._p.append(hyperlink)
            parent = hyperlink
            in_link = True
            continue
        if token.type == "link_close":
            parent = paragraph._p
            in_link = False
            continue
        if token.type not in ("text", "code_inline", "softbreak", "hardbreak"):
            raise ValueError(f"Unhandled inline token: {token.type}")
        run = Run(OxmlElement("w:r"), paragraph)
        parent.append(run._r)
        if token.type == "softbreak":
            run.text = " "
        elif token.type == "hardbreak":
            run.text = "\n"
        else:
            run.text = token.content
        run.bold = bold
        run.italic = italic
        if token.type == "code_inline":
            run.font.name = "Courier New"
            run.font.size = Pt(10)
        if in_link:
            run.font.color.rgb = ACCENT
            run.underline = True


def add_figure(document, width, image, number):
    paragraph = document.add_paragraph()
    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
    paragraph.paragraph_format.keep_with_next = True
    paragraph.paragraph_format.space_before = Pt(8)
    image_path = (SOURCE.parent / image.attrGet("src")).resolve()
    shape = paragraph.add_run().add_picture(str(image_path), width=width)
    shape._inline.docPr.set("descr", image.content)
    paragraph = document.add_paragraph(f"Figure {number}. {image.content}", style="Caption")
    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER


def add_table(document, width, tokens):
    rows = []
    row = None
    for token in tokens:
        if token.type == "tr_open":
            row = []
            rows.append(row)
        elif token.type == "inline":
            row.append(token.children)
    table = document.add_table(rows=len(rows), cols=len(rows[0]))
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    table.style = "Table Grid"
    numeric_columns = len(rows[0]) - 1
    numeric = min(0.21, 0.76 / numeric_columns)
    widths = [width * (1 - numeric * numeric_columns)] + [width * numeric] * numeric_columns
    for column, column_width in zip(table.columns, widths):
        column.width = int(column_width)
    borders = OxmlElement("w:tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        border = OxmlElement(f"w:{edge}")
        border.set(qn("w:val"), "single")
        border.set(qn("w:sz"), "4")
        border.set(qn("w:color"), "D8E2E8")
        borders.append(border)
    table._tbl.tblPr.append(borders)
    for row_index, (table_row, content_row) in enumerate(zip(table.rows, rows)):
        row_properties = table_row._tr.get_or_add_trPr()
        row_properties.append(OxmlElement("w:cantSplit"))
        if row_index == 0:
            row_properties.append(OxmlElement("w:tblHeader"))
        cells = zip(table_row.cells, content_row, widths)
        for column_index, (cell, children, column_width) in enumerate(cells):
            cell.width = int(column_width)
            cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
            paragraph = cell.paragraphs[0]
            paragraph.paragraph_format.space_before = Pt(5)
            paragraph.paragraph_format.space_after = Pt(5)
            paragraph.paragraph_format.line_spacing = 1.05
            if column_index == 0:
                paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
            else:
                paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT
            add_inline(paragraph, children)
            for run in paragraph.runs:
                run.font.size = Pt(9.3)
                run.bold = row_index == 0
            if row_index == 0:
                shade = OxmlElement("w:shd")
                shade.set(qn("w:fill"), "E8F3F4")
                cell._tc.get_or_add_tcPr().append(shade)
    document.add_paragraph().paragraph_format.space_after = Pt(0)


def add_article(document, width, tokens):
    index = 0
    figure_number = 0
    while index < len(tokens):
        token = tokens[index]
        if token.type == "heading_open":
            inline = tokens[index + 1]
            document.add_paragraph(inline.content, style=HEADING_STYLES[token.tag])
            index += 3
        elif token.type == "paragraph_open":
            inline = tokens[index + 1]
            children = inline.children or []
            image = next((child for child in children if child.type == "image"), None)
            if image:
                figure_number += 1
                add_figure(document, width, image, figure_number)
            else:
                paragraph = document.add_paragraph()
                add_inline(paragraph, children)
                if inline.content.startswith("#DragonflyDB"):
                    for run in paragraph.runs:
                        run.font.size = Pt(9)
                        run.font.color.rgb = MUTED
            index += 3
        elif token.type == "fence":
            paragraph = document.add_paragraph(token.content.rstrip("\n"), style="Article Code")
            shading = OxmlElement("w:shd")
            shading.set(qn("w:fill"), "F2F5F7")
            paragraph._p.get_or_add_pPr().append(shading)
            index += 1
        elif token.type == "table_open":
            end = next(
                pos for pos in range(index + 1, len(tokens)) if tokens[pos].type == "table_close"
            )
            add_table(document, width, tokens[index : end + 1])
            index = end + 1
        else:
            raise ValueError(f"Unhandled block token: {token.type}")


def add_notes(document):
    notes = document.add_paragraph("Benchmark notes", style="Heading 1")
    start = OxmlElement("w:bookmarkStart")
    start.set(qn("w:id"), "1")
    start.set(qn("w:name"), NOTES_BOOKMARK)
    notes._p.insert(1 if notes._p.pPr is not None else 0, start)
    end = OxmlElement("w:bookmarkEnd")
    end.set(qn("w:id"), "1")
    notes._p.append(end)
    for text in NOTES:
        document.add_paragraph(text)


def add_page_numbers(section):
    footer = section.footer.paragraphs[0]
    footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
    field = OxmlElement("w:fldSimple")
    field.set(qn("w:instr"), "PAGE")
    field_run = OxmlElement("w:r")
    field_text = OxmlElement("w:t")
    field_text.text = "1"
    field_run.append(field_text)
    field.append(field_run)
    footer._p.append(field)


def main():
    document = create_document()
    section = document.sections[0]
    width = section.page_width - section.left_margin - section.right_margin
    tokens = MarkdownIt("commonmark").enable("table").parse(SOURCE.read_text())
    add_article(document, width, tokens)
    add_notes(document)
    add_page_numbers(section)
    document.save(OUTPUT)
    print(f"Created {OUTPUT} ({OUTPUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
