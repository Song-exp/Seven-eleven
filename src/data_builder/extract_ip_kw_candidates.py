"""IP 키워드 LLM 추론 → CSV 출력.

Gemma(Ollama)가 각 IP명 + 기존 키워드를 보고,
이 IP와 콜라보했거나 연관 검색어로 묶일 만한 속성 키워드를 자유 생성.
생성 결과를 graph vocab(keyword_nodes)에 exact/substring 매칭해 후보 확정.
기존 IP 키워드와 중복은 제외하고 CSV로 저장.

실행:
    python -m src.data_builder.extract_ip_kw_candidates
    python -m src.data_builder.extract_ip_kw_candidates --top_k 15 --timeout 120
"""
from __future__ import annotations

import argparse
import os
import platform
import re
import time
from pathlib import Path

import pandas as pd
import requests

# ── 설정 ─────────────────────────────────────────────────────────
HIN_DIR  = "data/processed/hin"
OUT_CSV  = "data/processed/hin/ip_kw_candidates.csv"
TOP_K    = 10
TIMEOUT  = 120   # 초 (첫 콜드 로드 감안해 넉넉히)

GEMMA_MODEL = "gemma4:e4b"

_PROMPT = """\
[SYSTEM]
당신은 편의점 신제품 네트워크 분석 AI다.
주어진 IP(캐릭터·브랜드·셀럽·콘텐츠 등)와 관련해, 편의점 상품에 쓰일 만한 속성 키워드를 추출하라.
이 IP와 콜라보한 적 있거나, 연관 검색어로 나오거나, 이 IP를 표현하기 좋은 맛·식감·재료·컨셉·감성 단어를 나열해라.
[제약]
1. 일반 속성 단어만 (브랜드명·IP명 자체는 제외).
2. 쉼표 구분, 설명·따옴표·마침표 금지.
3. 기존 키워드와 중복 없이.
4. 15개 이내.
[예시]
IP명: 뽀로로 / 기존 키워드: 어린이,캐릭터
→ 출력: 딸기,달콤,우유,바나나,초코,귀여움,파랑,아이스크림,젤리,푸딩

[USER]
IP명: {ip_name}
기존 키워드: {existing_kws}\
"""


# ── Ollama 유틸 (serve.py와 동일 패턴) ───────────────────────────
def _ollama_base() -> str:
    if os.environ.get("OLLAMA_BASE_URL"):
        return os.environ["OLLAMA_BASE_URL"].rstrip("/")
    rel = platform.uname().release.lower()
    if "microsoft" in rel or os.path.exists("/proc/sys/fs/binfmt_misc/WSLInterop"):
        return "http://localhost:11434"
    return "http://localhost:11435"


def _ollama(prompt: str, timeout: int = TIMEOUT) -> str:
    try:
        r = requests.post(
            _ollama_base() + "/api/generate",
            json={"model": GEMMA_MODEL, "prompt": prompt,
                  "stream": False, "keep_alive": "30m",
                  "options": {"temperature": 0.2}},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception as e:
        return f"__ERROR__: {e}"


def _match_to_graph(terms: list[str], vocab: set[str]) -> list[str]:
    """serve._match_to_graph와 동일: exact → substring 양방향."""
    out, seen = [], set()
    vocab_list = list(vocab)
    for term in terms:
        term = term.strip()
        if len(term) < 2:
            continue
        if term in vocab:
            if term not in seen:
                seen.add(term); out.append(term)
            continue
        best = None
        for kw in vocab_list:
            if term in kw or kw in term:
                if best is None or abs(len(kw) - len(term)) < abs(len(best) - len(term)):
                    best = kw
        if best and best not in seen:
            seen.add(best); out.append(best)
    return out


# ── 메인 ────────────────────────────────────────────────────────
def run(top_k: int = TOP_K, timeout: int = TIMEOUT,
        hin_dir: str = HIN_DIR, out_csv: str = OUT_CSV):
    hin = Path(hin_dir)
    ip_nodes = pd.read_parquet(hin / "ip_nodes.parquet")
    vocab    = set(pd.read_parquet(hin / "keyword_nodes.parquet")["keyword"])

    rows = []
    total = len(ip_nodes)
    for i, (_, row) in enumerate(ip_nodes.iterrows(), 1):
        ip_name  = row["ip_name"]
        ip_kws   = list(row["키워드_final"]) if isinstance(row["키워드_final"], list) else []
        ip_kw_set = set(ip_kws)

        print(f"[{i:3d}/{total}] {ip_name} ...", end=" ", flush=True)

        raw = _ollama(
            _PROMPT.format(ip_name=ip_name, existing_kws=", ".join(ip_kws)),
            timeout=timeout,
        )

        if raw.startswith("__ERROR__"):
            print(f"오류: {raw}")
            rows.append({"ip": ip_name, "현재키워드": ", ".join(ip_kws),
                         "추가할키워드": "", "비고": raw})
            continue

        # 쉼표·줄바꿈·슬래시 분리 → graph vocab 매칭
        terms    = [t.strip() for t in re.split(r"[\s,/·\n]+", raw) if len(t.strip()) >= 2]
        matched  = _match_to_graph(terms, vocab)
        # 이미 IP에 있는 키워드 제외, 최대 top_k개
        filtered = [k for k in matched if k not in ip_kw_set][:top_k]

        print(f"→ {len(filtered)}개: {', '.join(filtered) if filtered else '없음'}")
        rows.append({
            "ip":        ip_name,
            "현재키워드": ", ".join(ip_kws),
            "추가할키워드": ", ".join(filtered),
            "비고":       "" if filtered else "후보 없음",
        })

        time.sleep(0.3)   # Ollama 부하 방지

    df = pd.DataFrame(rows)[["ip", "현재키워드", "추가할키워드", "비고"]]
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    n_with = (df["추가할키워드"].str.strip() != "").sum()
    total_added = df["추가할키워드"].apply(
        lambda v: len([x for x in str(v).split(",") if x.strip()])
    ).sum()
    print(f"\n저장: {out_csv}")
    print(f"후보 있는 IP: {n_with}/{len(df)}개  /  총 후보: {total_added}개")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top_k",   type=int, default=TOP_K)
    parser.add_argument("--timeout", type=int, default=TIMEOUT)
    parser.add_argument("--hin_dir", default=HIN_DIR)
    parser.add_argument("--out_csv", default=OUT_CSV)
    args = parser.parse_args()
    run(args.top_k, args.timeout, args.hin_dir, args.out_csv)
