# -*- coding: utf-8 -*-
"""
keyword_pipeline.py — 4-step 키워드 정제 파이프라인
노트북 Phase 0 셀에서 추출하여 외부 스크립트에서 import 가능하게 모듈화.

Usage:
    from src.data_builder.keyword_pipeline import run_pipeline
"""
import ast
import importlib
import os
import sys
from collections import OrderedDict

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "eda"))

import keyword_rules
importlib.reload(keyword_rules)

# ── 규칙 로드 (PATCH 병합 포함) ───────────────────────────────────────────────
REMOVE_KWS        = set(keyword_rules.REMOVE_KWS) | keyword_rules.PATCH_REMOVE
SPLIT_MAP         = {**keyword_rules.SPLIT_MAP,   **keyword_rules.PATCH_SPLIT}
SANDWICH_PRODUCTS = set(keyword_rules.SANDWICH_PRODUCTS)
CONTAINS_COLLAPSE = list(keyword_rules.CONTAINS_COLLAPSE)
EXTRA_SPLIT_MAP   = dict(keyword_rules.EXTRA_SPLIT_MAP)
EXTRA_SYNONYM_MAP = dict(keyword_rules.EXTRA_SYNONYM_MAP)
REMOVE_WORDS      = set(keyword_rules.REMOVE_WORDS)
KEYWORD_MAPPING   = {**keyword_rules.KEYWORD_MAPPING, **keyword_rules.PATCH_DECOMPOSE}

SYNONYM_MAP = {**keyword_rules.SYNONYM_MAP, **keyword_rules.PATCH_SYNONYM}
SYNONYM_MAP.update({
    kw: code
    for code, kws in keyword_rules.PROMO_MAP.items()
    for kw in kws
})


# ── Step 1: 노이즈 키워드 제거 ─────────────────────────────────────────────────
def remove_noise_kws(kw_list: list) -> list:
    if not isinstance(kw_list, list):
        return kw_list
    return [kw for kw in kw_list if kw not in REMOVE_KWS]


# ── Step 2: 동의어 통합 + 분할 ─────────────────────────────────────────────────
def unify_keywords(kw_list: list) -> list:
    if not isinstance(kw_list, list):
        return kw_list
    result = []
    for kw in kw_list:
        if kw in SPLIT_MAP:
            result.extend(SPLIT_MAP[kw])
        else:
            result.append(SYNONYM_MAP.get(kw, kw))
    return list(dict.fromkeys(result))


# ── Step 3: 고급 규칙 (CONTAINS_COLLAPSE, EXTRA_SPLIT, REMOVE_WORDS 등) ───────
def apply_advanced_rules(kw_list: list, product_name: str = '') -> list:
    if not isinstance(kw_list, list):
        return kw_list
    result = []
    for kw in kw_list:
        if kw == '제로':
            if product_name and any(p in product_name for p in SANDWICH_PRODUCTS):
                result.append(kw)
            continue
        if kw in EXTRA_SPLIT_MAP:
            result.extend(EXTRA_SPLIT_MAP[kw])
            continue
        collapsed = next((w for w in CONTAINS_COLLAPSE if w in kw), None)
        if collapsed:
            result.append(collapsed)
            continue
        if kw in REMOVE_WORDS:
            continue
        result.append(EXTRA_SYNONYM_MAP.get(kw, kw))
    return list(dict.fromkeys(result))


# ── Step 4: 키워드 매핑 (1→N 분해 포함) ────────────────────────────────────────
def apply_keyword_mapping(kw_list) -> list:
    if isinstance(kw_list, str):
        try:
            kw_list = ast.literal_eval(kw_list)
        except Exception:
            kw_list = [kw_list]
    if not isinstance(kw_list, list):
        return []
    expanded = []
    for kw in kw_list:
        expanded.extend(KEYWORD_MAPPING.get(kw, [kw]))
    return list(OrderedDict.fromkeys(expanded))


# ── 통합 파이프라인 ─────────────────────────────────────────────────────────────
def run_pipeline(kw_list, product_name: str = '') -> list:
    kw_list = remove_noise_kws(kw_list)
    kw_list = unify_keywords(kw_list)
    kw_list = apply_advanced_rules(kw_list, product_name)
    kw_list = apply_keyword_mapping(kw_list)
    return kw_list
