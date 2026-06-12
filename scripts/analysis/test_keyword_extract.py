import sys
import os
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src", "data_builder"))

from keyword_extractor import extract_keywords_from_name, MODEL_NAME

print(f"모델: {MODEL_NAME}\n")

tests = [
    "롯데)초코바나나맛아이스크림70ml",
    "레몬에이드젤리100g",
    "PB)직화불고기버거",
    "NEW)딸기생크림케이크",
    "산리오)헬로키티딸기우유200ml",
    "2배더진한아메리카노500ml",
    "삼각김밥참치마요",
    "롯데)스크류바50ml",
]

total_start = time.time()
for name in tests:
    t0 = time.time()
    result = extract_keywords_from_name(name)
    elapsed = time.time() - t0
    print(f"[{elapsed:.1f}s] {name} → {result}")

print(f"\n총 소요: {time.time() - total_start:.1f}s")
