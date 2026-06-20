# Method B — 실험 현황 및 설계

## 1. 베이스라인 확정

| exp | kw_min | ip_min | h | 새 엣지 | test PR-AUC | 비고 |
|---|---|---|---|---|---|---|
| exp18 | 5 | 2 | 128 | — | 0.6681 | 멀티홉 1-어텐션 최초 |
| exp22 | 3 | 2 | 128 | — | 0.6744 | kw 임계 완화 |
| **exp21** | **5** | **2** | **64** | **—** | **0.6867** | **★ 현재 베이스라인** |

> exp21이 exp22를 역전한 것은 새 네트워크(IP-IP 엣지) 추가 후 재학습 결과.  
> exp21과 exp22의 차이는 **kw_min (5 vs 3)** + **hidden_dim (64 vs 128)** 두 가지.

---

## 2. 새 네트워크에서 추가된 엣지

기존 HIN에서 제품 P의 1홉 이내로 도달하지 못했던 경로:

| 관계 | 경로 | 행렬 | 새 엣지 타입 |
|---|---|---|---|
| P-I-K | 제품 → IP → 키워드 | A_PI @ A_IK | `has_kw_via_ip` |
| P-I-IP-K | 제품 → IP → IP → 키워드 | A_PI @ A_II @ A_IK | `has_kw_ipip` |
| P-K-K | 제품 → 키워드 → 트렌드속성키워드 | A_PK @ A_KK | `has_kw_trend` |

> 세 경로 모두 사전 계산(pre-computed)으로 접힌 뒤 product→keyword 엣지로 추가.  
> 1-layer GNN이 1홉 어텐션만으로 이 경로의 정보를 흡수할 수 있음.

---

## 3. Ablation 실험 설계 (exp32-35)

**방식**: OFAT (One-Factor-At-a-Time) — 요인 하나씩 변경, 나머지 고정  
**순서**: 구조 변경(엣지) → 그래프 밀도(ip 임계) → 모델 용량(hidden_dim)

```
exp21 (baseline, 0.6867)
  │
  ├─ Phase 1: 엣지 추가 여부  (ip=2, h=64 고정)
  │      exp32: +has_kw_via_ip +has_kw_ipip        (K-K 없음)
  │      exp33: +has_kw_via_ip +has_kw_ipip +has_kw_trend  (K-K 포함)
  │
  ├─ Phase 2: ip_min_shared 2→1  (h=64, Phase1 winner 엣지 고정)
  │      exp34: Phase1 winner + ip=1
  │
  └─ Phase 3: hidden_dim 64→128  (Phase1+2 winner 설정 고정)
         exp35: Phase2 winner + h=128
```

### Phase 1 판단 기준
- exp32 > exp21: 새 경로(P-I-K, P-I-IP-K)가 유의미
- exp33 > exp32: K-K 트렌드 경로도 유효 (EDA 기준 예측은 exp32 우세 — K-K 제거 시 val +0.009)
- exp33 < exp32: K-K는 노이즈 → K-K 없는 설정 유지

### Config 파일

| exp | config 파일 | Phase | 변경 사항 |
|---|---|---|---|
| exp32 | `exp32_via_ip_ipip.yaml` | 1a | +via_ip +ipip (trend off) |
| exp33 | `exp33_via_ip_ipip_trend.yaml` | 1b | +via_ip +ipip +trend |
| exp34 | `exp34_ip1.yaml` | 2 | Phase1 winner + ip=1 |
| exp35 | `exp35_h128.yaml` | 3 | Phase2 winner + h=128 |

> **exp34 주의**: Phase 1에서 exp33(K-K on)이 이겼다면  
> `exp34_ip1.yaml`의 `add_trend_kw_edges: false → true` 수정 후 실행.
>
> **exp35 주의**: Phase 2에서 ip=2가 유리했다면  
> `exp35_h128.yaml`의 `hop2_ip_min_shared: 1 → 2` 수정 후 실행.

---

## 4. 실행 방법

**노트북**: `experiments/notebooks/methodB_edge_ablation.ipynb`

| 셀 | 내용 |
|---|---|
| 1 | 설계 개요 (markdown) |
| 2 | 환경 설정, `run_exp()` / `load_metrics()` 유틸 정의 |
| 3 | `run_exp('exp32')` |
| 4 | `run_exp('exp33')` |
| 5 | Phase 1 비교표 + Phase 2 base 안내 |
| 6 | `run_exp('exp34')` |
| 7 | Phase 2 비교표 + Phase 3 base 안내 |
| 8 | `run_exp('exp35')` |
| 9 | Phase 3 비교표 |
| 10 | 전체 요약 테이블 + 최종 winner |

**실행 순서**:
1. 셀 1~5 실행 (Phase 1 완료 후 결과 확인)
2. 필요시 `exp34_ip1.yaml` 수정
3. 셀 6~7 실행 (Phase 2 완료 후 결과 확인)
4. 필요시 `exp35_h128.yaml` 수정
5. 셀 8~10 실행

---

## 5. 코드 변경 이력

| 파일 | 변경 내용 |
|---|---|
| `src/data_builder/build_hetero_data.py` | `add_via_ip_edges`, `add_ipip_kw_edges`, `add_trend_kw_edges` 파라미터 추가 및 행렬곱 계산 블록 |
| `src/train/trainer.py` | `build_graph()` 호출에 위 3개 파라미터 pass-through |

---

## 6. 전체 실험 이력 (Method B)

> 상세 수치는 `experiments/notebooks/methodB_metapath_search.ipynb` 참조

| exp | 핵심 변경 | test PR-AUC | 상태 |
|---|---|---|---|
| exp18 | 멀티홉 1-어텐션 최초 (kw5, ip2, h128) | 0.6681 | 완료 |
| exp21 | hidden_dim 128→64 | **0.6867** | ★ 베이스라인 |
| exp22 | kw_min 5→3 | 0.6744 | 완료 |
| exp23 | kw_min 5→8 | 0.6755 | 완료 |
| exp25 | 2-layer | 0.6736 | 완료 |
| exp27 | 1-hop 퇴화 | 0.6802 | 완료 (추천 불가) |
| exp32 | +via_ip +ipip | — | **예정** |
| exp33 | +via_ip +ipip +trend | — | **예정** |
| exp34 | Phase1 winner + ip=1 | — | **예정** |
| exp35 | Phase2 winner + h=128 | — | **예정** |
