"""MD 처방 파이프라인 (exp47 leak-free).

단일 추론 캐시 → 집계·차분 행렬 → 처방 엔진 → 검증·KPI.
계획: docs/eda_channel_prescription_plan.md (Plan 2), docs/final_model_leakfree_switch_plan.md (Plan 1).
"""
from .engine import MDEngine, EngineConfig

__all__ = ["MDEngine", "EngineConfig"]
