# HIN-GNN 실험 환경 및 프레임워크

> 생성일: 2026-06-04  
> 대상: `experiments/` 디렉토리 아래의 ablation·재학습 실험 관리 전반

---

## 1. 디렉토리 구조

```
experiments/
├── configs/                      ← 실험별 하이퍼파라미터 YAML
│   ├── exp01_baseline.yaml
│   ├── exp02_alpha_tuning.yaml
│   ├── exp04_offline_copurchase.yaml
│   ├── exp05_quick_copurchase.yaml
│   ├── exp06_both_copurchase.yaml       ← Lift × α_r
│   └── exp07_copurchase_binary.yaml     ← Binary × α_r
│
├── notebooks/                    ← 실험별 실행·결과 노트북
│   ├── exp01_baseline.ipynb
│   ├── exp02_alpha_tuning.ipynb
│   ├── exp06_both_copurchase.ipynb
│   ├── exp07_copurchase_binary.ipynb
│   └── compare_copurchase.ipynb         ← exp01/06/07 통합 비교
│
├── results/                      ← 실험 실행 시 자동 생성
│   └── {exp_name}/
│       ├── hin_gnn_best.pt                        (체크포인트)
│       ├── config_used.yaml                       (실제 사용된 config 사본)
│       ├── metrics.json                           (train/val/test 지표)
│       ├── relation_importance.json               (DiffMG α_r 층별)
│       ├── weighted_product_keyword_edges.parquet (가중 네트워크)
│       ├── learned_product_scores.parquet         (제품별 성공확률)
│       ├── report.md                              (자동 생성 리포트)
│       ├── training_curve.png                     (노트북 실행 시 생성)
│       └── alpha_heatmap.png                      (노트북 실행 시 생성)
│
└── exp_utils.py                  ← 실험 오케스트레이션 유틸리티
```

---

## 2. 핵심 유틸리티 함수 (`experiments/exp_utils.py`)

| 함수 | 역할 |
|---|---|
| `run_experiment(config_path, exp_name, overrides, force)` | config → train → eval → export 원스텝 실행 |
| `load_experiment(exp_name)` | 저장된 결과 dict 복원 (재학습 없이) |
| `compare_experiments([exp1, exp2, ...])` | 지표 비교 DataFrame 반환 |
| `plot_alpha_heatmap(exp_name)` | DiffMG α_r 층별 히트맵 시각화 |
| `plot_training_curve(history)` | val PR-AUC 학습 곡선 시각화 |
| `print_metrics_table(results)` | train/val/test 지표 콘솔 출력 |
| `print_recommendations(results)` | 시드 키워드별 추천 결과 출력 |

### `run_experiment()` 내부 흐름

```
config YAML 로드
    ↓ overrides 패치 (있는 경우)
config_used.yaml 저장 (experiments/results/{exp_name}/)
    ↓
src.train.trainer.train()  ← hin_gnn_best.pt 저장
    ↓
src.eval.export_results.export_experiment()
    ├── metrics 계산 (train/val/test)
    ├── weighted_product_keyword_edges.parquet
    ├── learned_product_scores.parquet
    ├── relation_importance.json
    └── report.md
    ↓
metrics.json 저장
결과 dict 반환
```

### 결과 dict 구조

```python
{
    "metrics": {
        "train": {"pr_auc": float, "auc_roc": float, "f1": float, "threshold": float},
        "val":   {...},
        "test":  {...}
    },
    "rel_importance": [
        {"product__has_kw__keyword": float, ...},   # layer 0
        {...}                                        # layer 1
    ],
    "recs": {
        "마라": [("유산슬", 0.024), ...],
        "로제": [...],
        ...
    },
    "history": [{"epoch": int, "loss_w": float, "pr_auc": float, ...}, ...]
}
```

---

## 3. 실험 목록

### exp01 — Baseline

| 항목 | 값 |
|---|---|
| 목적 | `docs/training_decisions.md` 확정 세팅 재현·비교 기준점 |
| config | `experiments/configs/exp01_baseline.yaml` |
| 주요 세팅 | hidden=128, layers=2, heads=4, lr_w=0.005, lr_α=0.005, temperature=1.0, complement=False |
| 기존 결과 | test PR-AUC=0.4844, AUC-ROC=0.7789 |
| 문제점 | α_r 균등(≈1/8=0.125) → DiffMG 관계 게이팅 미분화 상태 |

### exp02 — DiffMG α 튜닝

| 항목 | 값 |
|---|---|
| 목적 | α_r 균등 문제 해결 → 관계별 중요도 실질 분화 유도 |
| config | `experiments/configs/exp02_alpha_tuning.yaml` |
| 변경점 | `train.lr_alpha`: 0.005 → **0.02** / `model.diffmg_temperature`: 1.0 → **0.5** |
| 근거 | temperature ↓ → softmax 집중도 ↑. lr_alpha ↑ → α 파라미터 빠른 수렴. |
| 가설 | α_r 분산 증가 → 유의미한 관계 집중 → PR-AUC 개선 |

### exp06 — 동반구매 엣지 Lift × α_r

| 항목 | 값 |
|---|---|
| 목적 | Lift 가중 동반구매 엣지(오프라인+퀵커머스)가 NPD 예측에 기여하는지 측정 |
| config | `experiments/configs/exp06_both_copurchase.yaml` |
| 변경점 | `co_offline` + `co_quick` 엣지 추가, `use_lift_weights: true` |
| 데이터 | `offline_commerce_edge_lift_pair_out.csv` (968쌍) / `quick_commerce_edge_lift_pair_out.csv` (761쌍) |
| 비교 대상 | exp01(baseline), exp07(binary) |

### exp07 — 동반구매 엣지 Binary × α_r

| 항목 | 값 |
|---|---|
| 목적 | Lift값 자체의 신호 기여 검증 — 엣지 존재 여부만으로도 충분한지 비교 |
| config | `experiments/configs/exp07_copurchase_binary.yaml` |
| 변경점 | exp06과 동일 구조, `use_lift_weights: false`만 다름 |
| 비교 대상 | exp01(baseline), exp06(Lift×α_r) |
| 실행 노트북 | `experiments/notebooks/compare_copurchase.ipynb` |

---

## 4. 코드 변경 사항 (2026-06-04)

### `src/data_builder/build_hetero_data.py`

- `build_graph()` 파라미터 추가: `include_complement: bool = False`, `complement_path: str`
- `include_complement=True` 시 `complement_lift_pairs.csv` 로드 → `("product", "complement", "product")` 엣지 추가
- `forward_edge_index_dict()` 하드코딩 제거 → `data.edge_types`에서 `rev_` 접두 제외 동적 추출로 변경

```python
# 변경 전
def forward_edge_index_dict(data):
    fwd = [("product", "has_kw", "keyword"), ...]
    return {et: data[et].edge_index for et in fwd}

# 변경 후
def forward_edge_index_dict(data):
    return {et: data[et].edge_index for et in data.edge_types if not et[1].startswith("rev_")}
```

### `src/train/trainer.py`

- `build_graph()` 호출 시 `include_complement=cfg["graph"].get("include_complement_edges", False)` 전달

### `src/eval/export_results.py`

- `export_experiment(ckpt_path, out_dir, data_dir, sample_seeds)` 함수 추가
  - 체크포인트·출력 디렉토리를 파라미터로 받아 실험별 독립 저장 지원
  - `_write_md()` 에 `md_path` 파라미터 추가 (기본값: `docs/hin_gnn_results.md` 유지)
- 기존 `main()` 동작 무변경

---

## 5. 실험 실행 방법

### 노트북 실행 (권장)

각 실험 노트북을 열어 셀 전체 실행:

```
experiments/notebooks/exp01_baseline.ipynb
experiments/notebooks/exp02_alpha_tuning.ipynb
experiments/notebooks/exp03_complement_edges.ipynb
```

노트북 내 핵심 셀:
```python
results = run_experiment('experiments/configs/expXX_*.yaml', 'expXX_*')
```

### 터미널 실행 (스크립트)

```python
from experiments.exp_utils import run_experiment
results = run_experiment('experiments/configs/exp02_alpha_tuning.yaml', 'exp02_alpha_tuning')
```

### 결과만 로드 (재학습 없이)

```python
from experiments.exp_utils import load_experiment, compare_experiments
results = load_experiment('exp01_baseline')
df = compare_experiments(['exp01_baseline', 'exp02_alpha_tuning', 'exp03_complement_edges'])
```

### config 일부만 override (임시 실험)

```python
results = run_experiment(
    'experiments/configs/exp02_alpha_tuning.yaml',
    'exp02_temp0.3',
    overrides={'model.diffmg_temperature': 0.3}
)
```

---

## 6. 평가 지표 해석

| 지표 | 설명 | 기준 |
|---|---|---|
| PR-AUC | Precision-Recall AUC (주지표) | 랜덤 기준선 **0.236** (양성 23.6%) |
| AUC-ROC | ROC AUC | 랜덤 기준선 0.500 |
| F1@best | best-threshold F1 | - |

- test PR-AUC ≥ 0.47 (랜덤의 2배) → 유의미한 학습 신호
- α_r 표준편차 > 0.01 → DiffMG 관계 게이팅 작동 판단

---

## 7. 새 실험 추가 방법

1. `experiments/configs/expXX_*.yaml` 작성 (기존 config 복사 후 변경점만 수정)
2. `experiments/notebooks/expXX_*.ipynb` 작성 (기존 노트북 복사 후 헤더·변경점 수정)
3. 이 문서 섹션 3에 실험 항목 추가
4. `docs/training_decisions.md` 에 결정 로그 추가

---

## 8. 실험 실행 규칙

노트북 작성 규칙은 `docs/data_schema.md` **실험 실행 규칙** 섹션 참조.

요약:
- 같은 역할 변수 → 실험 번호 접미사 (`r01`, `r06`, `r07`)
- 반복 코드 → setup 셀에 함수로 추출 (`save_fig` 등)
- 비교 실험 → `compare_XXX.ipynb` 통합 노트북, 섹션별 분리
