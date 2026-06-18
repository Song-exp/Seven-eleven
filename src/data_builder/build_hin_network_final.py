"""HIN 키워드 정제 → 최종 네트워크 생성 (04 노트북과 독립 실행).

흐름:
    원본 parquet 로드(Read-Only)
      → keyword_nodes_검토_마무리_final.xlsx Sheet1 규칙 + 51개 수동 보정 병합
      → 노드 키워드 리스트 정규화(제거/대체/분할)
      → 노드에서 엣지 재생성(explode)
      → keyword_nodes 재구성
      → data/processed/hin/*_final.parquet 저장

원본 7개 parquet은 절대 수정하지 않음 (CLAUDE.md §2 데이터 불변성).
규칙 의미론은 04 Phase 3.6과 동일: O=제거 / 단일값=대체 / 쉼표=분할 / 빈칸=유지.

실행:
    python -m src.data_builder.build_hin_network_final            # 저장
    python -m src.data_builder.build_hin_network_final --dry-run  # 리포트만
"""
from __future__ import annotations

import json
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd

HIN = Path("data/processed/hin")
XLSX = HIN / "keyword_nodes_검토_마무리_final.xlsx"
RULE_SHEET = "sheet1"  # Sheet1 = keyword_nodes_검토(작업중)

# ── 51개 수동 보정 (Sheet1 미수록 = 검수표에 없던 키워드) ─────────────────────
# 분할 잔여 조각(19) + 고유명/브랜드 조각(15) = 34개 제거
MANUAL_REMOVE = {
    # 분할 잔여 조각 의심
    "가슴", "팬", "테일", "코드", "코스", "피스", "테이블", "사이즈", "코인", "테크",
    "톡톡", "압도적", "덕후", "붉은말", "장수", "지구", "태양", "중국", "바다",
    # 고유명/브랜드 조각
    "라파게티", "알볼로", "제빵소", "경양", "스프린트", "악어", "응급실", "항아리",
    "균형", "기능", "검", "계장", "럭키", "산더미", "바틀",
}
# 오타·표기 교정(3) — 나머지 14개(조림·조리·지단·숙주·카스텔라·아이리쉬·모찌리도후·
#   상하·유바리·도쿠시마·히말라야·에이징·맥앤치즈·민물장어)는 규칙 없음 → 유지
MANUAL_RENAME = {"앙념": "양념", "케첩": "케찹", "오리지날": "오리지널"}

M = "{http://purl.oclc.org/ooxml/spreadsheetml/main}"  # Strict OOXML 네임스페이스


# ── 1. Strict-OOXML xlsx Sheet1 직접 파싱 ────────────────────────────────────
def parse_strict_sheet(xlsx: Path, sheet: str) -> pd.DataFrame:
    """pandas/openpyxl이 못 읽는 Strict xlsx에서 단일 시트를 DataFrame으로."""
    z = zipfile.ZipFile(xlsx)
    ss = [
        "".join(t.text or "" for t in si.iter(M + "t"))
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall(M + "si")
    ]
    root = ET.fromstring(z.read(f"xl/worksheets/{sheet}.xml"))
    rows: dict[int, dict[str, str]] = {}
    for c in root.iter(M + "c"):
        m = re.match(r"([A-Z]+)(\d+)", c.get("r"))
        col, r = m.group(1), int(m.group(2))
        t, v, isn = c.get("t"), c.find(M + "v"), c.find(M + "is")
        if t == "s" and v is not None:
            val = ss[int(v.text)]
        elif isn is not None:
            val = "".join(x.text or "" for x in isn.iter(M + "t"))
        elif v is not None:
            val = v.text
        else:
            val = ""
        rows.setdefault(r, {})[col] = val
    header = rows[min(rows)]
    cols = sorted({c for rd in rows.values() for c in rd}, key=lambda x: (len(x), x))
    data = [[rows[r].get(c, "") for c in cols] for r in sorted(rows) if r != min(rows)]
    df = pd.DataFrame(data, columns=[header.get(c, c) for c in cols])
    return df


# ── 2. 규칙 맵 구성 ──────────────────────────────────────────────────────────
def build_rule_maps() -> tuple[set, dict, dict]:
    df = parse_strict_sheet(XLSX, RULE_SHEET)
    kw_col = "keyword"
    rule_col = next(c for c in df.columns if "키워드" in c and c != "keyword")  # '뺄 키워드'
    REMOVE: set[str] = set()
    RENAME: dict[str, str] = {}
    SPLIT: dict[str, list[str]] = {}
    for kw, rule in zip(df[kw_col].astype(str), df[rule_col].astype(str)):
        kw, rule = kw.strip(), rule.strip()
        if not kw:
            continue
        if rule == "":
            continue  # 유지
        if rule.upper() == "O":
            REMOVE.add(kw)
        elif "," in rule:
            SPLIT[kw] = [t.strip() for t in rule.split(",") if t.strip()]
        else:
            RENAME[kw] = rule
    # 51개 수동 보정 병합 (수동이 우선)
    REMOVE |= MANUAL_REMOVE
    for k, v in MANUAL_RENAME.items():
        RENAME[k] = v
        SPLIT.pop(k, None)
        REMOVE.discard(k)
        REMOVE.discard(v)  # 수동 대체 대상은 Sheet1 제거(O)보다 우선 — 생존 보장
        SPLIT.pop(v, None)
    # 대체 맵 fixpoint·순환 해소 → 항등(no-op) 제거
    RENAME = resolve_rename_map(RENAME)
    RENAME = {k: v for k, v in RENAME.items() if v != k}
    return REMOVE, RENAME, SPLIT


def resolve_rename_map(raw: dict) -> dict:
    """대체 체인을 fixpoint까지 해소. 순환(예: A↔B)은 사전순 최소값으로 병합.

    데이터 품질 이슈 방어: 검수표의 자기참조(A→A)·양방향 모순(A→B, B→A)을
    결정적으로 정규화하여 무한 루프/잔존을 방지.
    """
    resolved = {}
    for start in raw:
        seen, path, cur = set(), [], start
        while cur in raw:
            if cur in seen:  # 순환 탐지 → 사이클 멤버 중 최소값 대표
                cycle = path[path.index(cur):]
                cur = min(cycle)
                break
            seen.add(cur)
            path.append(cur)
            cur = raw[cur]
        resolved[start] = cur
    return resolved


# ── 3. 토큰 정규화 (제거 > 분할 > 대체 > 유지, 재귀로 체인·split결과 재적용) ──
def make_norm_token(REMOVE, RENAME, SPLIT):
    def norm_token(k, _depth: int = 0) -> list[str]:
        # RENAME은 build 단계에서 fixpoint·순환 해소 완료 → 1회 조회로 충분
        k = str(k).strip()
        if not k or k in REMOVE:
            return []
        if k in RENAME:
            k = RENAME[k]  # 이미 해소된 최종 대상
            if k in REMOVE:
                return []
        if k in SPLIT:
            out: list[str] = []
            for t in SPLIT[k]:
                t = t.strip()
                if t == k:  # 자기참조 방지
                    out.append(t)
                elif t:
                    out.extend(norm_token(t, _depth + 1))
            return out
        return [k]

    def norm_list(L) -> list[str]:
        if not isinstance(L, (list, np.ndarray)):
            return []
        seen, res = set(), []
        for k in L:
            for nk in norm_token(k):
                if nk not in seen:
                    seen.add(nk)
                    res.append(nk)
        return res

    return norm_token, norm_list


# ── 4. 메인 빌드 ─────────────────────────────────────────────────────────────
def build(dry_run: bool = False) -> dict:
    REMOVE, RENAME, SPLIT = build_rule_maps()
    norm_token, norm_list = make_norm_token(REMOVE, RENAME, SPLIT)
    stats = {"rules": {"remove": len(REMOVE), "rename": len(RENAME), "split": len(SPLIT)}}

    # 4-1 노드 정규화 (source of truth)
    prod = pd.read_parquet(HIN / "product_nodes.parquet")
    prod["키워드_final"] = prod["키워드_final"].apply(norm_list)
    prod["키워드_개수"] = prod["키워드_final"].apply(len)

    ipn = pd.read_parquet(HIN / "ip_nodes.parquet")
    ipn["키워드_final"] = ipn["키워드_final"].apply(norm_list)

    # 4-2 엣지 재생성 (노드에서 explode)
    pk = (
        prod[["ITEM_CD", "키워드_final"]]
        .explode("키워드_final")
        .dropna(subset=["키워드_final"])
        .rename(columns={"키워드_final": "keyword"})
        .drop_duplicates()
        .reset_index(drop=True)
    )
    ik = (
        ipn[["ip_name", "키워드_final"]]
        .explode("키워드_final")
        .dropna(subset=["키워드_final"])
        .rename(columns={"키워드_final": "keyword"})
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 4-3 트렌드 엣지: src/tgt 양쪽 정규화 + 행 확장 + self-loop 제거 + dedup
    te = pd.read_parquet(HIN / "trend_keyword_edges.parquet")
    te_rows = []
    for s, t in zip(te["src_keyword"], te["tgt_keyword"]):
        for ns in norm_token(s):
            for nt in norm_token(t):
                if ns != nt:
                    te_rows.append((ns, nt))
    te_final = (
        pd.DataFrame(te_rows, columns=["src_keyword", "tgt_keyword"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 4-4 keyword_nodes 재구성: 최종 그래프에 실제 등장하는 키워드 합집합 + 메타 이식
    final_kw = set(pk["keyword"]) | set(ik["keyword"])
    final_kw |= set(te_final["src_keyword"]) | set(te_final["tgt_keyword"])
    kn_old = pd.read_parquet(HIN / "keyword_nodes.parquet").set_index("keyword")
    kn = pd.DataFrame({"keyword": sorted(final_kw)})
    kn["is_trend_keyword"] = (
        kn["keyword"].map(kn_old["is_trend_keyword"]).fillna(0).astype("int64")
    )
    kn["추출_속성"] = kn["keyword"].map(
        lambda k: kn_old["추출_속성"][k] if k in kn_old.index else []
    )
    kn["추출_속성"] = kn["추출_속성"].apply(
        lambda v: list(v) if isinstance(v, (list, np.ndarray)) else []
    )
    kn["인스타_첫_등장일"] = kn["keyword"].map(
        lambda k: kn_old["인스타_첫_등장일"][k] if k in kn_old.index else None
    )

    # 4-5 product_ip_edges: 키워드 무관 → 그대로 복사
    pip = pd.read_parquet(HIN / "product_ip_edges.parquet")

    outputs = {
        "product_nodes_final.parquet": prod,
        "ip_nodes_final.parquet": ipn,
        "keyword_nodes_final.parquet": kn,
        "product_keyword_edges_final.parquet": pk,
        "ip_keyword_edges_final.parquet": ik,
        "trend_keyword_edges_final.parquet": te_final,
        "product_ip_edges_final.parquet": pip,
    }
    stats["shapes"] = {f: list(d.shape) for f, d in outputs.items()}

    # 4-6 검증
    node_kw = set(kn["keyword"])
    orphan_pk = set(pk["keyword"]) - node_kw
    orphan_ik = set(ik["keyword"]) - node_kw
    orphan_te = (set(te_final["src_keyword"]) | set(te_final["tgt_keyword"])) - node_kw
    leak_remove = node_kw & REMOVE
    leak_rename = node_kw & set(RENAME)
    stats["verify"] = {
        "orphan_product_kw": len(orphan_pk),
        "orphan_ip_kw": len(orphan_ik),
        "orphan_trend_kw": len(orphan_te),
        "remove_leak": len(leak_remove),
        "rename_src_leak": len(leak_rename),
    }
    assert not orphan_pk and not orphan_ik and not orphan_te, "고아 엣지 존재"
    assert not leak_remove, f"제거 키워드 잔존: {list(leak_remove)[:10]}"
    assert not leak_rename, f"대체 소스 잔존: {list(leak_rename)[:10]}"

    # 4-7 저장
    if not dry_run:
        for fname, d in outputs.items():
            d.to_parquet(HIN / fname, index=False)
        audit = {
            "remove": sorted(REMOVE),
            "rename": dict(sorted(RENAME.items())),
            "split": {k: SPLIT[k] for k in sorted(SPLIT)},
        }
        (HIN / "keyword_review_map_final.json").write_text(
            json.dumps(audit, ensure_ascii=False, indent=1), encoding="utf-8"
        )
    return stats


def main():
    dry = "--dry-run" in sys.argv
    stats = build(dry_run=dry)
    print(f"[규칙] 제거 {stats['rules']['remove']} / 대체 {stats['rules']['rename']} / 분할 {stats['rules']['split']}")
    print("[검증]", stats["verify"])
    print(f"[출력] {'(dry-run: 미저장)' if dry else 'data/processed/hin/*_final.parquet 저장'}")
    for f, sh in stats["shapes"].items():
        print(f"  {f}: {sh[0]} rows x {sh[1]} cols")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
