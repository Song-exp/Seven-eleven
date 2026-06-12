from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


BASE = Path("data/processed")
DEFAULT_SOURCE = BASE / "keyword_channel_frequency_review.xlsx"
DEFAULT_REVIEW = BASE / "keyword_channel_frequency_review_final.xlsx"
DEFAULT_OUTPUT = BASE / "keyword_channel_frequency_review_reviewed.xlsx"

SHEET_MAIN = "keyword_frequency"
COL_KEYWORD = "\ud0a4\uc6cc\ub4dc"
COL_NORMALIZED = "\uc815\uaddc\ud654"
FINAL_COLUMNS = [
    COL_KEYWORD,
    "\ube14\ub85c\uadf8 \ub4f1\uc7a5\ube48\ub3c4",
    "\uc778\uc2a4\ud0c0\uadf8\ub7a8 \ub4f1\uc7a5\ube48\ub3c4",
    "IP \ub4f1\uc7a5\ube48\ub3c4",
    "\ud2b8\ub80c\ub4dc \ub4f1\uc7a5\ube48\ub3c4",
    COL_NORMALIZED,
]


def read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name, dtype=object, keep_default_na=False)


def strip_text(value: object) -> str:
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def to_final_schema(df: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in FINAL_COLUMNS if column not in df.columns]
    if missing == [COL_NORMALIZED]:
        df = df.copy()
        df[COL_NORMALIZED] = ""
    elif missing:
        raise ValueError(f"source is missing required columns: {missing}")

    result = df[FINAL_COLUMNS].copy()
    result[COL_KEYWORD] = result[COL_KEYWORD].map(strip_text)
    result[COL_NORMALIZED] = result[COL_NORMALIZED].map(strip_text)
    result = result[result[COL_KEYWORD] != ""].drop_duplicates(COL_KEYWORD, keep="first")
    return result


def load_review_mapping(path: Path, sheet_name: str) -> dict[str, str]:
    review = read_sheet(path, sheet_name)
    missing = [column for column in [COL_KEYWORD, COL_NORMALIZED] if column not in review.columns]
    if missing:
        raise ValueError(f"review sheet {sheet_name!r} is missing columns: {missing}")

    mapping: dict[str, str] = {}
    conflicts: list[tuple[str, str, str]] = []

    for _, row in review[[COL_KEYWORD, COL_NORMALIZED]].iterrows():
        keyword = strip_text(row[COL_KEYWORD])
        normalized = strip_text(row[COL_NORMALIZED])
        if not keyword or not normalized:
            continue

        previous = mapping.get(keyword)
        if previous is not None and previous != normalized:
            conflicts.append((keyword, previous, normalized))
            continue
        mapping[keyword] = normalized

    if conflicts:
        examples = "; ".join(
            f"{keyword}: {old!r} vs {new!r}" for keyword, old, new in conflicts[:10]
        )
        raise ValueError(
            f"review mapping has {len(conflicts)} conflicting keyword values. "
            f"Examples: {examples}"
        )

    return mapping


def write_excel(df: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=SHEET_MAIN)
        worksheet = writer.book[SHEET_MAIN]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions
        for column, width in {
            "A": 24,
            "B": 16,
            "C": 20,
            "D": 14,
            "E": 16,
            "F": 24,
        }.items():
            worksheet.column_dimensions[column].width = width


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert keyword_channel_frequency_review.xlsx to the final review schema "
            "and apply existing normalization decisions by keyword."
        )
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--review", type=Path, default=DEFAULT_REVIEW)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--source-sheet", default=SHEET_MAIN)
    parser.add_argument("--review-sheet", default=SHEET_MAIN)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source = to_final_schema(read_sheet(args.source, args.source_sheet))
    mapping = load_review_mapping(args.review, args.review_sheet)

    before_nonblank = int((source[COL_NORMALIZED] != "").sum())
    source[COL_NORMALIZED] = source[COL_KEYWORD].map(mapping).fillna(source[COL_NORMALIZED])
    after_nonblank = int((source[COL_NORMALIZED] != "").sum())
    matched = int(source[COL_KEYWORD].isin(mapping).sum())

    write_excel(source, args.output)

    print(f"saved={args.output}")
    print(f"rows={len(source):,}")
    print(f"review_mapping={len(mapping):,}")
    print(f"matched_keywords={matched:,}")
    print(f"normalization_nonblank={before_nonblank:,}->{after_nonblank:,}")
    print(f"unmatched_source_keywords={len(source) - matched:,}")


if __name__ == "__main__":
    main()
