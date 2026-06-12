"""
enrich_keywords_from_name.py
제품명 → LLM 키워드 추출 → 기존 키워드와 합집합

처리 흐름:
  product_final_keywords.csv
    → 각 제품명에 extract_keywords(제품명) 호출
    → LLM 추출 키워드 ∪ 기존 키워드
    → product_final_keywords_enriched.csv 저장

체크포인트:
  keyword_enrich_checkpoint.csv (ITEM_CD | llm_keywords | done)
  — 중단 후 재실행 시 완료된 항목 스킵
"""
import io
import sys
import os
import time

import pandas as pd
import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
print("[시작] enrich_keywords_from_name.py", flush=True)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src", "data_builder"))

from keyword_extractor import extract_keywords_from_name

# ── 경로 ──────────────────────────────────────────────────────────────────────
BASE      = os.path.join(PROJECT_ROOT, "data", "processed", "hin", "제품별키워드처리")
INPUT     = os.path.join(BASE, "product_final_keywords.csv")
OUTPUT    = os.path.join(BASE, "product_final_keywords_enriched.csv")
CKPT      = os.path.join(BASE, "keyword_enrich_checkpoint.csv")

CHECKPOINT_EVERY = 50   # N개마다 체크포인트 저장
RETRY_DELAY      = 2    # LLM 오류 시 재시도 대기(초)
MAX_RETRY        = 2    # 최대 재시도 횟수


# ── 유틸 ──────────────────────────────────────────────────────────────────────
def parse_kw(v) -> list[str]:
    if pd.isna(v) or str(v).strip() == "":
        return []
    return [x.strip() for x in str(v).split(",") if x.strip()]


def merge_kw(existing: list[str], llm: list[str]) -> list[str]:
    """순서 유지하며 합집합 (기존 우선, LLM 추가분 뒤에 붙임)."""
    seen = set(existing)
    result = list(existing)
    for kw in llm:
        if kw and kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result


# ── 체크포인트 로드 ────────────────────────────────────────────────────────────
if os.path.exists(CKPT):
    ckpt_df = pd.read_csv(CKPT, encoding="utf-8-sig")
    done_ids = set(ckpt_df["ITEM_CD"].astype(str).tolist())
    ckpt_map = ckpt_df.set_index("ITEM_CD")["llm_keywords"].to_dict()
    print(f"[체크포인트 로드] 완료된 항목: {len(done_ids)}개")
else:
    done_ids = set()
    ckpt_map = {}
    print("[체크포인트 없음] 처음부터 시작")

# ── 입력 로드 ─────────────────────────────────────────────────────────────────
r = pd.read_csv(INPUT, encoding="utf-8-sig")
print(f"[로드] 총 {len(r)}개 제품")

total     = len(r)
n_skip    = 0
n_process = 0
n_added   = 0
ckpt_buf  = []   # 미저장 체크포인트 버퍼

# ── 메인 루프 ─────────────────────────────────────────────────────────────────
results = []
for i, row in r.iterrows():
    item_cd  = str(row["ITEM_CD"]) if pd.notna(row["ITEM_CD"]) else ""
    prod_nm  = str(row["제품명"])
    cur_kws  = parse_kw(row["키워드"])
    ip_val   = row["IP"] if pd.notna(row["IP"]) else ""

    # 이미 처리된 경우 체크포인트에서 복원
    if item_cd in done_ids:
        llm_raw  = ckpt_map.get(item_cd, "")
        llm_kws  = parse_kw(llm_raw)
        merged   = merge_kw(cur_kws, llm_kws)
        results.append({
            "ITEM_CD": item_cd,
            "제품명":   prod_nm,
            "키워드":   ", ".join(merged),
            "IP":      ip_val,
        })
        n_skip += 1
        continue

    # LLM 호출 (재시도 포함)
    t0 = time.time()
    llm_kws = []
    for attempt in range(MAX_RETRY + 1):
        llm_kws = extract_keywords_from_name(prod_nm)
        if llm_kws:
            break
        if attempt < MAX_RETRY:
            time.sleep(RETRY_DELAY)
    elapsed = time.time() - t0

    merged   = merge_kw(cur_kws, llm_kws)
    added    = len(merged) - len(cur_kws)
    n_added += added
    n_process += 1
    seq = n_skip + n_process

    results.append({
        "ITEM_CD": item_cd,
        "제품명":   prod_nm,
        "키워드":   ", ".join(merged),
        "IP":      ip_val,
    })

    # 체크포인트 버퍼
    ckpt_buf.append({
        "ITEM_CD":      item_cd,
        "llm_keywords": ", ".join(llm_kws),
        "done":         True,
    })
    done_ids.add(item_cd)

    # 진행 출력 (매 항목)
    added_str = f" (+{added})" if added > 0 else ""
    print(f"[{seq:4d}/{total}] {elapsed:5.1f}s  {prod_nm[:30]:<30}  {llm_kws}{added_str}")

    # 체크포인트 저장
    if len(ckpt_buf) >= CHECKPOINT_EVERY:
        _new = pd.DataFrame(ckpt_buf)
        if os.path.exists(CKPT):
            _old = pd.read_csv(CKPT, encoding="utf-8-sig")
            _combined = pd.concat([_old, _new], ignore_index=True).drop_duplicates("ITEM_CD", keep="last")
        else:
            _combined = _new
        _combined.to_csv(CKPT, index=False, encoding="utf-8-sig")
        ckpt_buf = []

# 남은 체크포인트 저장
if ckpt_buf:
    _new = pd.DataFrame(ckpt_buf)
    if os.path.exists(CKPT):
        _old = pd.read_csv(CKPT, encoding="utf-8-sig")
        _combined = pd.concat([_old, _new], ignore_index=True).drop_duplicates("ITEM_CD", keep="last")
    else:
        _combined = _new
    _combined.to_csv(CKPT, index=False, encoding="utf-8-sig")

# ── 최종 저장 ─────────────────────────────────────────────────────────────────
out_df = pd.DataFrame(results)
out_df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")

print(f"\n[완료]")
print(f"  처리: {n_process}개 / 스킵(체크포인트): {n_skip}개")
print(f"  추가된 키워드 건수: {n_added}개")
print(f"  저장: {OUTPUT}")
