import ast
import re
from collections import Counter
from pathlib import Path

import pandas as pd


BASE = Path("data/processed")
OUT = BASE / "keyword_channel_frequency_review.xlsx"

COL_KEYWORD = "\ud0a4\uc6cc\ub4dc"
COL_KEYWORD_CLEAN = "\ud0a4\uc6cc\ub4dc_\uc815\uc81c"
COL_STORE = "\ud3b8\uc758\uc810\uba85"
COL_SEVEN = "\uc138\ube10\uc77c\ub808\ube10"
COL_TREND_KEYWORD = "\ud2b8\ub80c\ub4dc_\ud0a4\uc6cc\ub4dc"
COL_TREND_ATTRS = "\ucd94\ucd9c_\uc18d\uc131"


def iter_keywords(value):
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass

    if isinstance(value, (list, tuple, set)):
        values = list(value)
    elif hasattr(value, "tolist") and not isinstance(value, str):
        values = value.tolist()
    else:
        s = str(value).strip()
        if not s or s in ("nan", "none", "null", "[]"):
            return []
        # ['로 시작하면 regex로 직접 추출
        # ast.literal_eval 먼저 쓰면 numpy repr ['a' 'b']를 ['ab']로 잘못 합성함
        if s.startswith("["):
            items = re.findall(r"'([^']*)'", s)
            if items:
                values = items
            else:
                values = [s]
        else:
            try:
                result = ast.literal_eval(s)
                values = list(result) if isinstance(result, (list, tuple)) else [str(result)]
            except Exception:
                values = s.split(",")

    keywords = []
    for item in values:
        if item is None:
            continue
        keyword = str(item).strip()
        if not keyword or keyword.lower() in {"nan", "none", "null"}:
            continue
        keywords.append(keyword)
    return keywords


def count_from_list_column(path, column):
    df = pd.read_parquet(path)
    counter = Counter()
    for value in df[column]:
        counter.update(iter_keywords(value))
    return counter


def main():
    blog_counter = count_from_list_column(
        BASE / "blog_keywords_with_pos.parquet", COL_KEYWORD
    )

    insta = pd.read_parquet(BASE / "instagram_engagement_with_keywords.parquet")
    if COL_STORE in insta.columns:
        insta = insta[insta[COL_STORE].astype(str).str.strip().eq(COL_SEVEN)].copy()

    insta_counter = Counter()
    for _, row in insta.iterrows():
        keywords = iter_keywords(row.get(COL_KEYWORD_CLEAN))
        if not keywords:
            keywords = iter_keywords(row.get(COL_KEYWORD))
        insta_counter.update(keywords)

    ip = pd.read_parquet(BASE / "ip_keywords.parquet")
    ip_counter = Counter()
    if "ip_name" in ip.columns:
        for ip_name in ip["ip_name"]:
            ip_counter.update(iter_keywords([ip_name]))
    for keywords in ip[COL_KEYWORD]:
        ip_counter.update(iter_keywords(keywords))

    trend = pd.read_parquet(BASE / "trend_keywords.parquet")
    trend_counter = Counter()
    if COL_TREND_KEYWORD in trend.columns:
        for keyword in trend[COL_TREND_KEYWORD]:
            trend_counter.update(iter_keywords([keyword]))
    if COL_TREND_ATTRS in trend.columns:
        for attrs in trend[COL_TREND_ATTRS]:
            trend_counter.update(iter_keywords(attrs))

    all_keywords = sorted(
        set(blog_counter) | set(insta_counter) | set(ip_counter) | set(trend_counter)
    )

    freq_cols = [
        "\ube14\ub85c\uadf8 \ub4f1\uc7a5\ube48\ub3c4",
        "\uc778\uc2a4\ud0c0\uadf8\ub7a8 \ub4f1\uc7a5\ube48\ub3c4",
        "IP \ub4f1\uc7a5\ube48\ub3c4",
        "\ud2b8\ub80c\ub4dc \ub4f1\uc7a5\ube48\ub3c4",
    ]

    result = pd.DataFrame(
        {
            "\ud0a4\uc6cc\ub4dc": all_keywords,
            freq_cols[0]: [blog_counter.get(k, 0) for k in all_keywords],
            freq_cols[1]: [insta_counter.get(k, 0) for k in all_keywords],
            freq_cols[2]: [ip_counter.get(k, 0) for k in all_keywords],
            freq_cols[3]: [trend_counter.get(k, 0) for k in all_keywords],
            "\uc815\uaddc\ud654": ["" for _ in all_keywords],
        }
    )

    result["_channel_count"] = (result[freq_cols] > 0).sum(axis=1)
    result["_total_freq"] = result[freq_cols].sum(axis=1)
    result = result.sort_values(
        ["_channel_count", "_total_freq", "\ud0a4\uc6cc\ub4dc"],
        ascending=[False, False, True],
    )
    result = result[
        [
            "\ud0a4\uc6cc\ub4dc",
            freq_cols[0],
            freq_cols[1],
            freq_cols[2],
            freq_cols[3],
            "\uc815\uaddc\ud654",
        ]
    ]

    with pd.ExcelWriter(OUT, engine="openpyxl") as writer:
        result.to_excel(writer, index=False, sheet_name="keyword_frequency")
        worksheet = writer.book["keyword_frequency"]
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

    print(f"saved={OUT}")
    print(f"keywords={len(result):,}")
    print(
        "unique_by_channel="
        f"blog:{len(blog_counter):,}, "
        f"instagram:{len(insta_counter):,}, "
        f"ip:{len(ip_counter):,}, "
        f"trend:{len(trend_counter):,}"
    )
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
