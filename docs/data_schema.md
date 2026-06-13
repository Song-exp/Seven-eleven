# 세븐일레븐 NPD 데이터 스키마

## 전체 파이프라인

```
[Step 1]  eda/notebooks/00_product_keyword_pipeline.ipynb
          ↓
          원천(인스타 raw xlsx, 블로그 raw csv, B4, B2_POS_SALE)을 받아
          소스별 키워드 전처리 + 수동 검수 반영 + 인스타 engagement 빌드

          중간 파일:
          ├── B4_ITEM_DV_INFO_filtered.parquet  (식품 카테고리 필터 + is_npd 플래그)
          ├── insta_keywords_processed.parquet  (인스타 제품별 확정 키워드)
          ├── blog_keywords_processed.parquet   (블로그 제품별 확정 키워드)
          ├── trend_keywords_processed.parquet  (트렌드 제품별 확정 키워드 + trend_flag_kws)
          └── ip_master_dataset.parquet         (IP별 최종 키워드 + 소속 커뮤니티 원천)

          ★ 최종 출력:
          ├── instagram_engagement_with_keywords.parquet  (편의점 3사 게시물 × 키워드)
          ├── ip_keywords.parquet                         (Block 13 생성 → ip-patch 셀로 수동 편집 일괄 적용)
          └── trend_keywords.parquet                      (트렌드 키워드 → 추출 속성 key:value)

[Step 1.5]  eda/notebooks/07_seven_keyword_pool_review.ipynb  ★ 완료
            ↓
            세븐일레븐 인스타 키워드 수동 검수 및 정제
            smart_clean_result_seven_final.xlsx (검수 완료 파일) 필요

            중간 파일:
            └── eda/seven_insta_unmatched_products.csv  (원본명 기준 미매칭 제품 목록)

            ★ 최종 출력 (instagram_engagement_with_keywords.parquet 업데이트):
            └── 키워드_정제 컬럼 추가 (세븐일레븐 전용, Phase 8)

[Step 1.6]  src/data_builder/patch_insta_product_names.py  ★ 완료 (최초 1회)
            ↓
            CU/GS25 원본명 교정 (N종 분리, 오표기 수정, 행사문구 삭제)

            ★ 출력 (갱신):
            └── instagram_engagement_with_keywords_final.csv  (CU/GS25 패치 적용 작업 파일)

            eda/notebooks/01b_matching_diagnostics.ipynb  (cu-gs-norm-* 셀, Step 3 실행 시 자동 적용)
            ↓
            CU/GS25 정규화명 클러스터 단위 통일 + 비식품 행 제거

            입력: eda/product_name_cluster_review_final.xlsx  (수동 검수 완료)

            ★ 최종 출력 (갱신):
            └── instagram_engagement_with_keywords.parquet  (CU/GS25: 식품만 유지, 정규화명 클러스터 통일)
                instagram_engagement_with_keywords_final.csv  (동일 내용 CSV)

[Step 1.7]  키워드 정규화 검수 및 적용  ★ 완료 (2026-06-04)
            ↓
            전 소스(블로그·인스타·IP·트렌드) 키워드 빈도 집계 → 수동 검수 → 각 소스에 정규화 반영

            스크립트:
            ├── scripts/create_keyword_channel_frequency_review.py
            │         → data/processed/keyword_channel_frequency_review.xlsx  (빈도 집계)
            ├── scripts/migrate_keyword_normalization.py
            │         → data/processed/keyword_frequency_review_final.xlsx  (기존 정규화 이식)
            └── src/data_builder/apply_keyword_normalization.py
                      → 각 소스 parquet에 _final 컬럼 추가
                      ★ 01b의 `apply-kw-norm` 셀에서 subprocess로 자동 호출 (별도 실행 불필요)
                        이유: 01b의 cu-gs-norm 셀이 CU/GS25 parquet을 재구성하면서
                              키워드_final 컬럼을 초기화하므로, 01b 종료 전 재적용이 필요

            정규화 규칙 (keyword_frequency_review_final.xlsx '정규화' 컬럼 기준):
              O        → 해당 키워드 삭제
              값 (쉼표 없음) → 1:1 대체 (RENAME)
              A, B (쉼표)   → 분리 후 대체 (SPLIT)
              NaN      → 원본 유지

            ★ 최종 출력 (각 소스 parquet에 컬럼 추가):
            ├── blog_keywords_with_pos.parquet      → `키워드_final`
            ├── instagram_engagement_with_keywords.parquet → `키워드_final`
            ├── ip_keywords.parquet                 → `키워드_final`
            └── trend_keywords.parquet              → `추출_속성_final`

            정규화 결과 요약 (원본 → final):
              블로그 1,328 → 1,084개 / 인스타 1,476개 / IP 757 → 717개 / 트렌드 518 → 453개
              전체 합집합 2,946 → 2,489개 (Δ -457)

[Step 2]  eda/notebooks/01_pos_feature_engineering.ipynb
          ↓
          B4 NPD 상품 목록 기반 POS 30일 피처 계산

          ★ 최종 출력:
          └── pos_product_features.parquet         (NPD 30일 판매·프로모션 피처 + 타겟 레이블)

[Step 3]  eda/notebooks/01b_matching_diagnostics.ipynb
          ↓
          Step 1~2 출력을 입력으로 받아
          소스별 데이터 품질 QA + 수동 매칭 보정 + 커버리지 분석

          ★ 최종 출력 (사이드 이펙트):
          └── instagram_engagement_with_keywords.parquet 에 ITEM_CD 컬럼 write-back
              (인스타만 7개 제품 중 pool 매칭 성공한 제품에 한해)

          참고: seven_eleven_product_master.parquet, blog_keywords_with_pos.parquet,
                pos_b4_insta_pool.csv 는 01b 이전 버전에서 생성된 파일로 현재 디스크에 존재.
                재생성이 필요한 경우 01b git history 참조.
```

> **실행 순서**: `patch_insta_product_names.py` (최초 1회) → 00 → 07 → 01 → **01b** (내부에서 `apply_keyword_normalization.py` 자동 실행 포함) → 04
> 수동 검수 단계(Phase 2, 4)가 00 중간에 있으므로 00은 검수 완료 파일이 있을 때만 전체 실행 가능.
> 07은 세븐일레븐 인스타 키워드 검수 완료 파일(`smart_clean_result_seven_final.xlsx`)이 있을 때 실행.
> Step 1.6(`product_name_cluster_review_final.xlsx`)은 CU/GS25 원본명 검수 완료 파일이 있을 때 실행 (01b 내 자동 적용).
> `apply_keyword_normalization.py`는 01b의 `apply-kw-norm` 셀에서 subprocess로 자동 호출됨 — 별도 실행 불필요.

---

## 최종 데이터 9종 요약

| 파일 | 역할 | 생성 위치 |
|---|---|---|
| `pos_product_features.parquet` | NPD 상품 30일 판매·프로모션 피처 + 타겟 레이블 | 01 (phase1d-save) |
| `blog_keywords_with_pos.parquet` | 블로그 키워드 × ITEM_CD (POS-블로그 연결) | 01b (bridge-builder, 파일만 존재) |
| `instagram_engagement_with_keywords.parquet` | 편의점 3사 게시글 단위 인스타 데이터 + 세븐일레븐 키워드_정제 + ITEM_CD | 00 (phase4d) + 07 Phase 8 + Step 1.6 + 01b write-back |
| `seven_eleven_product_master.parquet` | 세븐일레븐 POS ↔ 인스타 채널 간 제품명 키 매핑 브릿지 | 01b (bridge-builder, 파일만 존재) |
| `ip_keywords.parquet` | IP → 키워드 key:value | 00 (block13) |
| `trend_keywords.parquet` | 트렌드 키워드 → 추출 속성 key:value | 00 (block13) |
| `pos_b4_insta_pool.csv` | 전체 POS + 인스타 통합 풀 (01b 커버리지 분석 입력) | 01b (bridge-builder, 파일만 존재) |
| `npd_success_labels.csv` | 3사 합집합 기준 성공/실패 라벨 (5,143개, 편의점명 포함) | 01b (`cu-gs-insta-success-label`) |
| `complement_lift_pairs.csv` | B2 영수증 동반구매 보완재 쌍 (968쌍, Lift ≥ 3.0) | 외부 분석 결과 → `data/processed/` 수동 배치 |

---

## HIN 그래프 파일 6종 (`data/processed/hin/최종/`)

> **04_hin_graph_builder.ipynb** 실행 결과물. GNN 학습 입력으로 직접 사용.
> 실제 출력 수량은 04 Phase 8 통계 요약 셀 기준.
> ⚠ 구버전 `data/processed/hin/` 경로의 파일은 사용하지 않음.

| 파일 | 내용 | 비고 |
|---|---|---|
| `product_nodes.parquet` | 제품 노드 (세븐 + CU + GS25) | 프로모션 원핫 18컬럼 포함 |
| `ip_nodes.parquet` | IP 노드 | 282개 |
| `keyword_nodes.parquet` | 키워드 노드 | keyword_eda_final.csv 정규화 적용 후 재구성 |
| `product_keyword_edges.parquet` | 제품 → 키워드 엣지 | |
| `ip_keyword_edges.parquet` | IP → 키워드 엣지 | |
| `trend_keyword_edges.parquet` | 트렌드키워드 → 속성키워드 엣지 | |
| `product_ip_edges.parquet` | 제품 → IP 엣지 | |

---

## IP 정규화 검수 파일 — `data/processed/hin/product_ip_mapping.xlsx`

> IP 명칭 통일 및 제품-IP 매핑 검수 결과물 (2026-06-10). 원본 데이터 수정 없이 별도 생성.
> 생성 과정 상세: `data/processed/hin/product_ip_mapping_README.md`

### Sheet 구성

| 시트명 | 내용 | 수량 |
|---|---|---|
| `제품_IP_매핑` | 정규화된 제품명 × IP명 매핑. 소스 컬럼(B/C/B+C) 포함 | 1,210행 / 1,048개 제품 / 205개 IP |
| `IP별_키워드` | 정규화·검수 완료된 IP명 + 키워드_final | 270개 IP |

### HIN 파일로 활용 가능 여부

| 시트 | 대응 HIN 파일 | 활용 가능 여부 |
|---|---|---|
| `IP별_키워드` | `ip_nodes.parquet` | **직접 대체 가능** — ip_name + 키워드_final 포맷 일치 |
| `제품_IP_매핑` | `product_ip_edges.parquet` | **ITEM_CD 매핑 필요** — 현재 `제품명` 기준이므로 ITEM_CD join 후 사용 |

### IP별_키워드 시트에 반영된 변경사항

| 변경 유형 | 내용 |
|---|---|
| IP명 표기 통일 | `PLAVE`→`플레이브`, `K-LEAGUE`→`K리그`, `베이크하우스 405`→`베이크하우스405`, `FIFA 파니니`→`FIFA파니니`, `서울 자가에 대기업 다니는 김 부장 이야기`→`서울자가에대기업다니는김부장이야기` |
| IP 제거 | `비비고`, `비비랩`, `로로멜로`, `델리팜`, `델토리` (브랜드 키워드, IP 분류 불필요) |
| 키워드 수정 | `류수영` IP의 키워드_final에서 `류수영`, `흑백요리사` 제거 |
| 신규 IP 추가 | `급식대가`, `안성재`, `여경래` (키워드 포함) |

### 원본 소스 3개

| 역할 | 파일 |
|---|---|
| IP 마스터 기준 | `data/processed/hin/ip_nodes_검토최종.xlsx` Sheet 3 (ip_nodes_final, 272개) |
| 제품별 IP 파편 | `data/processed/hin/product_ip_promotion_analysis.csv` (`ip관련키워드` 컬럼) |
| 제품별 IP 레이블 | `data/processed/hin/npd_success_labels_with_IP.xlsx` (`IP` 컬럼) |

---

## 구조 개요

```
      [POS 도메인]
  pos_product_features.parquet
  (ITEM_CD 기준, NPD only)
        │                 │
        │ ITEM_CD          │ ITEM_CD → seven_eleven_product_master → 인스타_정규화명
        │                 │                                                │
        ▼                 │                                                ▼
[블로그 도메인]            │                                        [인스타 도메인]
blog_keywords_with_pos    │                            instagram_engagement_with_keywords.parquet
(ITEM_CD 기준)            └──────── [브릿지] ──────────(정규화명 기준, post-level, 편의점 3사)
                      seven_eleven_product_master.parquet


  [IP 도메인]                          [트렌드 도메인]
  ip_keywords.parquet                  trend_keywords.parquet
  ip_name → [키워드]                   트렌드_키워드 → [추출_속성]
```

join 방법:
- POS × 블로그: `ITEM_CD` 직접 join
- POS × 인스타: 브릿지(`seven_eleven_product_master`) 경유 — `ITEM_CD` → `인스타_정규화명` = `정규화명`

---

## 1. 브릿지 — `seven_eleven_product_master.parquet`

> 세븐일레븐 POS ↔ 인스타 채널 간 제품명 키 매핑 테이블.
> POS 상품명(브랜드 포함)과 인스타 정규화명(브랜드 제거)을 연결.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `ITEM_CD` | str | POS 상품코드 (FK → POS 도메인) |
| `상품명` | str | POS 기준 상품명 (브랜드 포함, ex. `롯데)리얼소프트에그샌드`) |
| `인스타_정규화명` | str / NA | 인스타 정규화명 (FK → 인스타 도메인), 인스타 미등장 시 NA |
| `대분류` | str | |
| `중분류` | str | |
| `소분류` | str | |
| `is_npd` | bool | NPD(신제품) 여부 |
| `POS_등장` | bool | POS 판매 이력 존재 여부 |
| `인스타_등장` | bool | 세븐일레븐 인스타 계정 게시물 등장 여부 |
| `소스` | str | `POS만` / `POS+인스타` / `인스타만` |

- 총 10,586개 (POS만 9,927 / POS+인스타 652 / 인스타만 7)

---

## 2. POS 도메인

### 2-1. `pos_product_features.parquet`

> 01 Phase 1D (`phase1d-save`) 생성. NPD 상품 한정. 첫 판매일 기준 30일 윈도우.  
> **NPD 포함 조건** (3단계 필터):  
> 1. 파레토 중분류 포함 *(예외: 막걸리·전통음료·기타전통주·와인·노벨티 5개 중분류는 2026-06-01 `step1_3_expand_npd.py`로 직접 편입)*  
> 2. `첫판매일 >= 2025-01-15`  
> 3. `sales_30d_amt > 0` (반품초과·취소 제품 제외)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `ITEM_CD` | str | POS 상품코드 |
| `ITEM_NM` | str | 상품명 |
| `ITEM_LRDV_NM` | str | 대분류명 |
| `ITEM_MDDV_NM` | str | 중분류명 |
| `ITEM_SMDV_NM` | str | 소분류명 |
| `생존여부` | str | `생존` / `단종` |
| `첫판매일` | datetime | POS 최초 판매일 |
| `sales_30d_qty` | float32 | 출시 30일 총 판매수량 |
| `sales_30d_amt` | float32 | 출시 30일 총 판매금액 |
| `sales_days_observed` | uint32 | 출시 30일 내 실제 판매일수 |
| `daily_velocity` | float32 | 일평균 판매수량 (qty / 30) |
| `promo_count_30d` | int64 | 출시 30일 내 프로모션 횟수 |
| `promo_names_30d` | str | 프로모션명 목록 |
| `promo_types_30d` | str | 프로모션 유형 목록 |
| `promo_categories_30d` | str | 프로모션 카테고리 목록 |
| `has_promo_30d` | bool | 출시 30일 내 프로모션 여부 |

- **총 3,006개** (기존 2,411개 + 5개 중분류 확장 595개, 2026-06-01 기준)

### 2-2. `blog_keywords_with_pos.parquet`

> 01b 이전 버전 셀(삭제됨)에서 생성 (파일로만 존재). 블로그 키워드 소스 독립 테이블.
> `blog_keywords_processed.parquet` (원본 2,011개 + 재추출 346개 = 2,348개) 기준으로
> B4 fuzzy 매칭 후 ITEM_CD가 귀속된 행만 포함. 총 2,022행.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `ITEM_CD` | str | POS 상품코드 (FK → pos_product_features) |
| `ITEM_NM` | str | B4 기준 POS 상품명 |
| `정규화명` | str | 블로그 원천의 제품명 (B4 fuzzy 매칭 키) |
| `키워드` | list[str] | 블로그에서 추출·검수된 키워드 (원본) |
| `키워드_final` | list[str] | Step 1.7 정규화 적용 후 키워드 (HIN 학습 사용) |

- ITEM_CD 매칭된 행만 포함 (미매칭 제외)
- join 키: `ITEM_CD` = `pos_product_features.ITEM_CD`
- 키워드 있음: 1,965개 / 키워드 없음(빈 리스트): 57개 (위스키·굿즈류 블로그 후기 부재)
- **⚠ 타입 주의**: PyArrow가 `list` 컬럼을 `numpy.ndarray`로 복원함. 체크 시 `isinstance(x, (list, np.ndarray))`로 처리 필요

---

## 3. 인스타 도메인 — `instagram_engagement_with_keywords.parquet`

> 00 Cell 43 초기 생성 → **07 Phase 8에서 `키워드_정제` 컬럼 추가 후 덮어쓰기 (완료)**.
> 게시물 단위 원본 데이터. 한 행 = 한 게시물 × 한 제품 언급.
> 편의점 3사(세븐일레븐·GS25·CU) 전체 포함.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `편의점명` | str | `세븐일레븐` / `GS25` / `CU` |
| `원본명` | str | 게시물 원본 제품명 |
| `정규화명` | str | 정규화된 제품명 (FK → 브릿지.인스타_정규화명) |
| `키워드` | list[str] | 게시물에서 추출한 키워드 (원본, 00 생성) |
| `키워드_정제` | list[str] / None | 07 검수 후 최종 키워드 (세븐일레븐 전용; GS25·CU는 None) |
| `키워드_final` | list[str] | Step 1.7 정규화 적용 후 키워드 (HIN 학습 사용). 세븐일레븐은 `키워드_정제` 기반, CU/GS25는 `키워드` 기반 |
| `제외` | str / None | 검수 시 제외 표시된 키워드 메모 |
| `다시 봐야할 거` | str / None | 검수 보류 항목 메모 |
| `좋아요 수` | int64 | 게시물 좋아요 수 |
| `언급일` | str | 게시물 작성일 (YYYY-MM-DD) |
| `url` | str | 게시물 URL |
| `body` | str | 게시물 본문 |
| `키워드_개수` | int64 | 키워드_정제 리스트 길이 (07 생성) |
| `ITEM_CD` | str / NA | POS 상품코드 (01b `qa-insta-only-manual` write-back; 세븐일레븐 인스타만 7개 제품 중 pool 매칭 성공한 제품만 채워짐) |

- **총 4,249행** (세븐일레븐 1,301 / GS25 1,489 / CU 1,459) ← 2026-06-03 Step 1.6 적용 후 기준
- 세븐일레븐 필터: `편의점명 == '세븐일레븐'`
- 한 제품이 여러 게시물에 등장하면 여러 행 존재
- **키워드 활용 시**: `키워드_final` 우선 사용 (Step 1.7 정규화 적용본). `키워드_final`이 없거나 빈 경우 세븐일레븐은 `키워드_정제` → `키워드` fallback
- **CU/GS25 주의**: Step 1.6 적용으로 식품류만 포함 (비식품·굿즈·앨범 등 제거). `정규화명`은 `product_name_cluster_review_final.xlsx` 기준 클러스터 단위로 통일됨

---

## 4. IP 도메인 — `ip_keywords.parquet`

> 00 Cell 51 (Block 13) 생성. `IP_키워드사전.json` (검수 완료) 기반.
> IP명이 추출 대상(key), 검수된 최종 키워드 전체가 값(value).
> `run_pipeline()` 정규화 적용.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `ip_name` | str | IP명 (key) |
| `키워드` | list[str] | 해당 IP의 최종 키워드 목록 (원본) |
| `키워드_final` | list[str] | Step 1.7 정규화 적용 후 키워드 (HIN 학습 사용) |

- **총 288개 IP** (키워드 있는 IP: 288개) ← 2026-06-03 ip_keywords 검토 반영 후 기준

```python
ip = pd.read_parquet('data/processed/ip_keywords.parquet')
ip_dict = ip.set_index('ip_name')['키워드_final'].to_dict()  # key:value (정규화 적용본)
```

---

## 5. 트렌드 도메인 — `trend_keywords.parquet`

> 00 Cell 51 (Block 13) 생성. `trend_kw_set.json` (747개) + `제품_속성.json` 기반.
> 트렌드 키워드가 추출 대상(key), 해당 키워드에 대해 추출된 속성이 값(value).
> `run_pipeline()` 정규화 적용.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `트렌드_키워드` | str | 트렌드 키워드 (key, standalone 목록으로도 활용) |
| `추출_속성` | list[str] | `flavor`+`texture`+`ingredients`+`tpo`+`product_category` 합집합; 미추출 시 빈 리스트 (원본) |
| `추출_속성_final` | list[str] | Step 1.7 정규화 적용 후 추출_속성 (HIN 학습 사용) |

- 총 747개 트렌드 키워드 (추출_속성 있음: 450개 / 빈 리스트: 297개)
- `트렌드_키워드` 자체는 정규화 대상 아님 — `추출_속성`(속성 키워드)만 정규화 적용

```python
trend = pd.read_parquet('data/processed/trend_keywords.parquet')
trend_keyword_list = trend['트렌드_키워드'].tolist()                             # standalone 목록
trend_dict = trend.set_index('트렌드_키워드')['추출_속성_final'].to_dict()       # key:value (정규화 적용본)
```

---

## 6. 커버리지 비교 — `pos_b4_insta_pool.csv`

> 01b bridge-builder 셀에서 생성 (파일로만 존재). 전체 POS 상품 + 인스타 미매칭 제품 통합 풀.
> 01b `cov-npd-insta` / `cov-non-npd-insta` 커버리지 분석 셀의 입력으로 사용.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `ITEM_CD` | str | POS 상품코드 (인스타_미매칭 행은 NA) |
| `상품명` | str | POS 기준 상품명 |
| `정규화명` | str | 인스타 정규화명 (인스타_미매칭 행에만 존재) |
| `대분류` | str | |
| `중분류` | str | |
| `소분류` | str | |
| `is_npd` | bool | NPD 여부 |
| `소스` | str | `POS_B4` / `인스타_미매칭` |

- 총 10,680개 (POS_B4: 10,579개 / 인스타_미매칭: 101개)
- `pos_b4_insta_pool_final.csv` (10,693행, 컬럼 `제거/이름변경` 포함)는 구버전 — 사용하지 않음

---

## 7. 성공 라벨 — `npd_success_labels.csv`

> 01b `insta-success-1500` 셀에서 생성.
> POS NPD(`_df_final`) ∪ 인스타 브릿지 매핑 제품(`_combined`) 합집합 기준.
> POS 파레토 80% 성공 OR 인스타 좋아요합 >= 임계값(기본 3,000) 중 하나라도 충족하면 성공.
> 소스: `pos_b4_insta_pool.csv` 기반으로 상품명·중분류·소분류 조회.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `ITEM_CD` | str | POS 상품코드 (CU·GS25는 `{편의점명}_{정규화명}` 합성 ID) |
| `상품명` | str | 세븐: POS 기준 상품명 / CU·GS25: 정규화명 |
| `편의점명` | str | `세븐일레븐` / `CU` / `GS25` |
| `중분류` | str | 중분류명 (세븐만 존재; CU·GS25는 NA) |
| `소분류` | str | 소분류명 (세븐만 존재; CU·GS25는 NA) |
| `소스` | str | `POS` / `POS+인스타` / `인스타` / `CU_인스타` / `GS25_인스타` |
| `성공여부` | str | `성공` / `실패` |

- 세븐일레븐 기존: 3,144개 (POS 2,492 / POS+인스타 514 / 인스타 138, 2026-06-01 기준)
- CU/GS25 추가: 인스타 좋아요합 >= 임계값 기준 라벨 (01b `cu-gs-insta-success-label` 셀 생성 예정, 수량 EDA 후 확정)
- `소스` 컬럼 설명:
  - `POS` = 세븐 pos_product_features NPD (인스타 미등장)
  - `POS+인스타` = 세븐 NPD + 인스타 브릿지 매칭
  - `인스타` = 세븐 인스타 브릿지에만 존재 (비NPD)
  - `CU_인스타` = CU 인스타 좋아요 기준 (POS 없음)
  - `GS25_인스타` = GS25 인스타 좋아요 기준 (POS 없음)
- 인코딩: `utf-8-sig` (Excel 직접 열기 가능)
- 인스타 성공 임계값은 `01b insta-success-1500` / `cu-gs-insta-success-label` 셀의 `INSTA_THRESHOLD` 변수로 조정 후 재실행
- 성공 기준 상세: [`docs/npd_category_coverage.md` 섹션 5](npd_category_coverage.md) 참조

---

## 조인 시 주의사항

### ITEM_CD leading zero 불일치

브릿지(`seven_eleven_product_master`)와 POS 테이블 간 ITEM_CD 포맷이 다름.

| 테이블 | 예시 |
|---|---|
| 브릿지 | `53047` (leading zero 없음) |
| pos_product_features | `053047` (leading zero 있음) |
| blog_keywords_with_pos | `053047` (leading zero 있음) |

raw join 시 95개 소실 발생. **반드시 정규화 후 join.**

```python
def norm_id(x):
    try: return str(int(float(x)))
    except: return str(x)
```

---

## 조인 예시

```python
bridge = pd.read_parquet('data/processed/seven_eleven_product_master.parquet')
pos    = pd.read_parquet('data/processed/pos_product_features.parquet')
blog   = pd.read_parquet('data/processed/blog_keywords_with_pos.parquet')
insta  = pd.read_parquet('data/processed/instagram_engagement_with_keywords.parquet')

def norm_id(x):
    try: return str(int(float(x)))
    except: return str(x)

bridge['ITEM_CD'] = bridge['ITEM_CD'].apply(norm_id)
pos['ITEM_CD']    = pos['ITEM_CD'].apply(norm_id)
blog['ITEM_CD']   = blog['ITEM_CD'].apply(norm_id)

# 세븐일레븐 인스타만 필터
insta_7 = insta[insta['편의점명'] == '세븐일레븐']

# POS × 블로그 (ITEM_CD 직접 join)
pos_blog = pos.merge(blog, on='ITEM_CD', how='left')

# POS × 인스타 (브릿지 경유)
bridge_insta = (
    bridge[bridge['인스타_정규화명'].notna()]
    .merge(insta_7, left_on='인스타_정규화명', right_on='정규화명', how='left')
    .merge(pos[['ITEM_CD']], on='ITEM_CD', how='left')
)
```

---

## 알려진 한계

- NPD 키워드 없음: 굿즈/노벨티·현장 즉석 제품·위스키 등으로 블로그·인스타 후기 크롤링 대상 외 (블로그 57개, 인스타 다수)
- 인스타 도메인은 편의점 공식 계정 기준이며 개인 게시물은 미포함
- 세븐일레븐 인스타 키워드 정제 완료 (07_seven_keyword_pool_review.ipynb Phase 8, `키워드_정제` 컬럼으로 저장)
- 트렌드 키워드 297개는 `제품_속성.json` 미등재 (추출_속성 빈 리스트)
- `seven_eleven_product_master.parquet`, `blog_keywords_with_pos.parquet`, `pos_b4_insta_pool.csv`는
  01b의 이전 버전 셀(삭제됨)에서 생성됨 — 재생성 필요 시 01b git history 참조
- `blog_keywords_with_pos.parquet`의 `키워드` 컬럼은 PyArrow 읽기 시 `numpy.ndarray` 타입으로 반환됨 — `isinstance(x, (list, np.ndarray))`로 처리
- `sales_30d_amt <= 0` 제품 총 **170개** NPD에서 제거됨 (기존 122개 + 5개 중분류 확장 시 48개 추가): 고가 위스키, 선물세트류, 행사상품(★), 호빵 찜기낱개 등
- 인스타 성공 기준: 좋아요합 >= 3,000 (`01b insta-success-1500` 셀의 `INSTA_THRESHOLD` 변수로 조정)
- `Seven_instagram_completed.xlsx` (`data/processed/편의점_instagram/`): NPD 2,280개에 대한 PB_YN / IP_BRAND_YN 수동 기록 파일. POS 3,006개 중 기존 246개 + 신규 595개 = 841개 미기록 (신규 5개 중분류 미포함)
- `IP_속성_수동입력.json` (`data/processed/IP_속성추출/`): 재추출 불가 IP 14개 수동 검토분. 00 Step 4.5에서 `IP_속성.json` 및 `IP_키워드사전.json`에 병합
- `ip_keywords.parquet` 수동 편집은 `src/data_builder/patch_ip_keywords_all.py`에서 관리. Block 13은 `IP_키워드사전.json` 기반으로 재생성하므로 반드시 ip-patch 셀까지 실행해야 최종 상태 반영됨
- `ip_keywords.parquet` 2026-06-03 검토 완료: 삭제(응답하라1985·무무도사·장채아·최재승·리복방구), 병합(캐치티니핑→티니핑, 투슬리스→드래곤길들이기 등), 키워드 수정(MLB KBO 제거, 조야·옐로우즈·유우카·별모아 등) 반영 → **288개** IP
- `instagram_engagement_with_keywords_final.csv`: CU/GS25 전용 작업 파일. `patch_insta_product_names.py` 패치 완료 상태. 세븐일레븐 미포함. 01b의 CU/GS25 정규화 셀이 이 파일을 소스로 parquet을 재구성함
- CU/GS25 비식품 행(굿즈·앨범·생활용품 등) 총 568개가 Step 1.6에서 제거됨 (parquet 기준 4,972 → 4,249행)
- `키워드_final` / `추출_속성_final`: Step 1.7 정규화 적용본. HIN 학습 시 원본(`키워드`, `추출_속성`) 대신 이 컬럼 사용 권장. `apply_keyword_normalization.py` 재실행 시 갱신됨
- `keyword_frequency_review_final.xlsx` (`data/processed/`): 키워드 정규화 검수 완료 파일. 정규화 규칙 수정 시 이 파일의 '정규화' 컬럼 수정 후 `apply_keyword_normalization.py` 재실행
- 인스타 `키워드_final`: 세븐일레븐은 `키워드_정제` 기반, CU/GS25는 `키워드` 기반으로 정규화 적용됨. `키워드_정제`가 None인 세븐일레븐 행은 `키워드` fallback 적용

---

## 노트북 역할

### `00_product_keyword_pipeline.ipynb`

4개 소스(인스타·블로그·트렌드·IP)의 키워드를 정제하여 HIN 학습용 소스 데이터를 생성하는 메인 파이프라인.

| Phase | 내용 |
|---|---|
| **Phase 0** | 환경 설정 — 라이브러리 임포트, `keyword_rules.py` 로드, B4 식품 카테고리 필터링, is_npd 플래그 계산 (`첫판매일 >= 2025-01-15`, burn-in 14일 기준, `>=` 포함) |
| **Phase 1A** | Instagram 키워드 전처리 — 원본 xlsx 파싱, smart_clean 검수 결과 병합, 4-step 정제 파이프라인 적용 |
| **Phase 1B** | 블로그 키워드 전처리 — 원본 CSV 파싱, `batch_rerecrawl_attrs.py` 재추출 결과 병합 |
| **Phase 1C** | 트렌드 키워드 전처리 — community_1hop_mapping CSV + 제품_속성 JSON 기반 4-step 정제 |
| **Phase 2** | 키워드 어휘 검토 Export *(수동 검수용, 최초 1회만)* |
| **Phase 3** | IP 속성 키워드 처리 — `IP_속성.json` + `IP_속성_재추출.json` → `IP_속성_통합.json` → `ip_keyword_vocab_review_final.xlsx` 반영 → `IP_키워드사전.json` |
| **Step 4.5** | IP 수동입력 병합 — `IP_속성_수동입력.json` (14개)을 `IP_속성.json` 및 `IP_키워드사전.json`에 패치 (Step 4 포맷 유지) |
| **Phase 4** | 수동 검수 반영 — `keyword_vocab_review_final.xlsx` 패치 → 3소스 parquet 업데이트 |
| **Phase 4D** | Engagement 데이터셋 빌드 — `instagram_engagement_with_keywords.parquet` 생성 |
| **Phase 4E** | Trend Engagement 데이터셋 빌드 — `trend_engagement_with_keywords.parquet` 생성 |
| **Block 12** | IP Master 구축 — ip_name 기준 GroupBy → `ip_master_dataset.parquet` |
| **Block 13** | 최종 저장 — `ip_keywords.parquet` + `trend_keywords.parquet` |
| **ip-patch** | IP 통합 패치 — `patch_ip_keywords_all.py` 실행 → 키워드 제거·IP 삭제·병합·수동 입력 일괄 적용 (Block 13 직후 자동 실행) |

수동 검수 단계(Phase 2, 4)가 중간에 있어 **검수 완료 파일이 있을 때만 전체 실행 가능**.

> **ip_keywords.parquet 수동 편집 관리**  
> 파이프라인을 처음부터 끝까지 돌려도 Block 13 → ip-patch 셀 순서로 항상 최종 상태가 보장됩니다.  
> 앞으로 수동 편집(키워드 제거·IP 삭제·병합·재추출 결과)이 생기면 `src/data_builder/patch_ip_keywords_all.py`만 수정하면 됩니다.  
> 섹션별 편집 위치: `REMOVALS`(키워드 제거) / `DELETE_IPS`(IP 삭제) / `MERGES`(병합·이름 변경) / `OVERRIDES`(키워드 전체 덮어쓰기)  
>  
> **CU/GS25 원본명·정규화명 수동 편집 관리**  
> 원본명 교정(분리·수정·삭제)은 `src/data_builder/patch_insta_product_names.py`에서 관리.  
> 정규화명 클러스터링은 `eda/product_name_cluster_review_final.xlsx` 검수 후 01b 실행으로 자동 적용.

---

### `01_pos_feature_engineering.ipynb`

B4 NPD 상품 목록 기반으로 POS 매출 30일 피처를 계산하는 노트북.

| Phase | 내용 |
|---|---|
| **Phase 0** | B4 NPD 상품 목록 로드 (`B4_ITEM_DV_INFO_filtered.parquet`, is_npd=True) |
| **Phase 1A** | 첫 판매일 계산 — POS 전체 73M행 lazy scan → 상품별 `영업일자` 최솟값 |
| **Phase 1B** | 30일 매출 집계 — 첫 판매일 기준 30일 윈도우 → `sales_30d_qty`, `sales_30d_amt`, `sales_days_observed`, `daily_velocity` |
| **Phase 1C** | B5 프로모션 — 첫 30일 내 행사 겹침 여부 → `promo_count_30d`, `has_promo_30d` |
| **Phase 1D** | 최종 병합 및 저장. **저장 직전 `sales_30d_amt <= 0` 제품 제거** (반품초과·취소 데이터 품질 이슈) → `pos_product_features.parquet`. 제거된 ITEM_CD는 `B4_ITEM_DV_INFO_filtered.parquet`의 `is_npd`도 `False`로 동기화. |

---

### `01b_matching_diagnostics.ipynb`

Step 1~2 출력물의 데이터 품질 QA 및 POS-인스타 커버리지 분석 노트북.
`00 + 07 + 01` 완료 후 실행.

| 섹션 | 내용 |
|---|---|
| **CU/GS25 정규화명 클러스터링** (`cu-gs-norm-*`) | `product_name_cluster_review_final.xlsx` 로드 → `원본명 → 정규화명` 매핑 빌드 → CU/GS25 `정규화명` 덮어쓰기 + DELETE 행 제거 → CSV·parquet 저장. **세븐일레븐 행은 건드리지 않음.** parquet CU/GS25는 패치된 CSV로 교체 (비식품 자동 제거). |
| **QA 1 — 인스타** | `instagram_engagement_with_keywords.parquet` shape / 편의점별 행수 / 세븐일레븐 `키워드_정제` 채움 현황 |
| **QA 2 — 블로그** | `blog_keywords_with_pos.parquet` 키워드 현황 / ITEM_CD 매칭률 / 키워드 없는 57개 목록 |
| **QA 3 — POS** | `pos_product_features.parquet` null 현황 / 수치 기초 통계 |
| **QA 4 — IP** | `ip_keywords.parquet` 키워드 현황 |
| **QA 5 — 트렌드** | `trend_keywords.parquet` 추출_속성 현황 |
| **QA 6 — 브릿지** | `seven_eleven_product_master.parquet` 소스별 분포 / NPD 매칭률 |
| **QA 7 — 브릿지 미매칭 상세** | 인스타만 7개 / POS 있음 but NPD 아님 168개 분류 |
| **수동 보정** | `MANUAL_NORM_MAP` 기반 인스타만 7개 → pool 매칭 → ITEM_CD 확보 후 `instagram_engagement_with_keywords.parquet`에 write-back |
| **커버리지 분석 1** | NPD × 인스타 등장 비율 (전체 + 중분류별 테이블) |
| **커버리지 분석 2** | 인스타 등장 but NPD 미포함 — ① POS 있음 but NPD 아님 (브릿지 기준) / ② POS 매칭 없음 (pool 기준) |
| **3채널 매칭 현황** (`npd-3ch-coverage`) | NPD 기준 POS / 인스타 / 블로그 채널별 매칭 제품 수 및 조합별 집계 |
| **비NPD 인스타 — 12/1 이후 필터** (`non-npd-dec-filter`) | 비NPD 인스타 등장 제품 중 첫판매일 >= 2025-12-01인 제품 목록 (14개, 커버리지 비교에서 제외) ← 5개 중분류 편입으로 16→14개 |
| **비NPD 커버리지 비교** (`cc74e3fc`) | 12/1 이후 제외한 53개 기준 소분류별 POS 신규 수 vs 인스타 등장 수. 비NPD 분석 기준: 첫판매일 >= 2025-01-15 ← 5개 중분류 편입으로 80→53개 |
| **PB 커버리지 대조** (`pb-coverage-load`) | `Seven_instagram_completed.xlsx` 로드 → POS NPD 3,006개 vs 엑셀 2,280개 대조 / PB_YN·IP_BRAND_YN 분포 / 미기록 목록 출력 |
| **성공 라벨 EDA — POS** (`pos-success-*`) | 중분류별 파레토 80% 성공 라벨 / 간식빵 캐릭터빵·나머지간식빵 분리 Case A·B 비교 / 확정 라벨 중분류별 분포 / 파레토 구조 Scatter |
| **성공 라벨 EDA — 인스타** (`insta-success-*`) | 인스타 등장 전체(NPD 여부 무관, 브릿지 매핑 기준) 게시물 좋아요 수 분포 / 제품별 합산·최대·평균 집계 / 임계값별 성공 제품 수 시뮬레이션 |
| **인스타 성공 기준 적용** (`insta-success-1500`) | 좋아요합 >= 임계값(기본 3,000) 성공 제품 목록 / 인스타 전용 성공 제품 소분류·중분류별 분석 / POS+인스타 합집합 성공 현황 / `npd_success_labels.csv` 저장 (세븐일레븐 기준) |
| **CU/GS25 좋아요 분포 EDA** (`cu-gs-insta-likes-eda`) | CU/GS25 각각 정규화명 기준 좋아요합·최대·평균 집계 / 임계값별 성공 제품 수 시뮬레이션 / 세븐일레븐과 분포 비교 → 임계값 결정 근거 확보 |
| **CU/GS25 성공 라벨 생성** (`cu-gs-insta-success-label`) | 좋아요합 >= INSTA_THRESHOLD 기준 CU/GS25 성공 라벨 생성 / ITEM_CD = `{편의점명}_{정규화명}` 합성 ID / 기존 `npd_success_labels.csv`에 소스 `CU_인스타`·`GS25_인스타` 행 append 후 덮어쓰기 |
| **CU/GS25 HIN 커버리지 검토** (`cu-gs-hin-coverage`) | 제품별 키워드_final 보유율 / IP_NM 커버리지 / 예상 product-keyword·product-IP 엣지 수 집계 |
| **키워드 정규화 자동 적용** (`apply-kw-norm`) | `src/data_builder/apply_keyword_normalization.py` subprocess 실행 — cu-gs-norm 셀이 초기화한 CU/GS25 `키워드_final` 재생성. 4개 소스 parquet에 `키워드_final` / `추출_속성_final` 추가. **01b Run All 시 자동 실행됨** |

---

## HIN 그래프 스키마

> **구현 노트북**: `eda/notebooks/04_hin_graph_builder.ipynb`  
> **출력 디렉토리**: `data/processed/hin/최종/`  
> **키워드 정규화 입력**: `data/processed/hin/최종/keyword_eda_final.csv` (`제거/검토` 컬럼)

---

### 노드 스키마

#### 제품 노드 — `hin/최종/product_nodes.parquet`

**PK**: `ITEM_CD`
- 세븐일레븐: POS 상품코드 (예: `53047`)
- CU/GS25: 합성 ID `{편의점명}_{정규화명}` (예: `CU_에그샌드`) — POS 코드 없음

| 컬럼 | 타입 | 소스 | 비고 |
|---|---|---|---|
| `ITEM_NM` | str | 세븐: `pos_product_features.ITEM_NM` / CU·GS25: `정규화명` | 검토용 |
| `편의점명` | str | `instagram_engagement.편의점명` | `세븐일레븐` / `CU` / `GS25` |
| `성공여부` | str | `npd_success_labels.csv` | 타겟 레이블 (`성공` / `실패`) |
| `성공_소스` | str | `npd_success_labels.소스` | `POS` / `POS+인스타` / `인스타` / `CU_인스타` / `GS25_인스타` |
| `첫_등장일` | date | 세븐: POS `첫판매일` 우선 → 인스타 min / CU·GS25: 인스타 `언급일` min | |
| `인스타_언급횟수` | int | `instagram_engagement` groupby 집계 | |
| `인스타_언급일자` | list[date] | `instagram_engagement.언급일` 목록 | |
| `insta_mention_30d` | int | `인스타_언급일자` 중 `첫_등장일 + 30일` 이내 건수 (파생) | POS 30일 윈도우와 관측 기간 통일 |
| `키워드_final` | list[str] | `product_final_keywords.csv` → Phase 3.6 정규화 적용 | 제품-키워드 엣지 원천 |
| `promo_*` × 18 | int8 | `product_promo_keywords.csv` 원핫 인코딩 (Phase 7.5) | 아래 프로모션 컬럼 목록 참조 |

**프로모션 원핫 컬럼 18종** (Phase 7.5, `product_promo_keywords.csv` 기반):

| 컬럼명 | 원본 유형 | 건수 |
|---|---|---|
| `promo_0101_번들증정` | B5 행사유형 | 402 |
| `promo_0102_콤보증정` | B5 행사유형 | 64 |
| `promo_0103_번들할인` | B5 행사유형 | 52 |
| `promo_0104_콤보할인` | B5 행사유형 | 250 |
| `promo_0106_단품할인` | B5 행사유형 | 453 |
| `promo_0107_묶음할인(구간)` | B5 행사유형 | 106 |
| `promo_0201_번들증정` | B5 행사형태 | 3 |
| `promo_0203_번들할인` | B5 행사형태 | 143 |
| `promo_0205_장바구니할인` | B5 행사형태 | 74 |
| `promo_0301_구독행사` | B5 행사형태 | 304 |
| `promo_1+1` | 묶음 행사 | 114 |
| `promo_2+1` | 묶음 행사 | 297 |
| `promo_2+2` | 묶음 행사 | 8 |
| `promo_3+1` | 묶음 행사 | 7 |
| `promo_5+1` | 묶음 행사 | 2 |
| `promo_6+1` | 묶음 행사 | 1 |
| `promo_7+1` | 묶음 행사 | 1 |
| `promo_10+1` | 묶음 행사 | 9 |

#### IP 노드 — `hin/최종/ip_nodes.parquet`

**PK**: `ip_name`

| 컬럼 | 타입 | 소스 |
|---|---|---|
| `ip_name` | str | `product_ip_mapping.xlsx` Sheet `IP별_키워드` |
| `키워드_final` | list[str] | 동일 시트 → Phase 3.6 정규화 적용 |

#### 키워드 노드 — `hin/최종/keyword_nodes.parquet`

**PK**: `keyword`  
Phase 3.6 정규화(제거·대체·분리) 후 product_nodes ∪ ip_nodes의 합집합으로 재구성.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `keyword` | str | PK |
| `is_trend_keyword` | bool | `trend_keywords.트렌드_키워드` 포함 여부 |
| `추출_속성` | list[str] | 트렌드 키워드인 경우 연결된 속성 키워드 목록 |
| `인스타_첫_등장일` | date / None | 트렌드 키워드인 경우만 채워짐 |

---

### 엣지 스키마

| 파일 | 관계 | src | tgt | 원천 |
|---|---|---|---|---|
| `hin/최종/product_keyword_edges.parquet` | 제품 → 키워드 | `ITEM_CD` | `keyword` | `product_nodes.키워드_final` explode |
| `hin/최종/ip_keyword_edges.parquet` | IP → 키워드 | `ip_name` | `keyword` | `ip_nodes.키워드_final` explode |
| `hin/최종/trend_keyword_edges.parquet` | 키워드 → 키워드 | `src_keyword` | `tgt_keyword` | `trend_keywords.추출_속성_final` explode |
| `hin/최종/product_ip_edges.parquet` | 제품 → IP | `ITEM_CD` | `ip_name` | `product_ip_mapping.xlsx` Sheet1 + `product_final_keywords.IP` 합집합 |
| `complement_lift_pairs.csv` *(소스)* | 제품 ↔ 제품 (보완재) | `ITEM_CD` | `ITEM_CD` | B2 영수증 동반구매 Lift 분석 / **hin/최종/ 미통합** |

---

### `04_hin_graph_builder.ipynb`

HIN 노드·엣지 테이블을 생성하는 노트북.  
**선행 조건**: `keyword_eda_final.csv`의 `제거/검토` 컬럼 검수 완료 후 실행.  
처음부터 끝까지 Run All 하면 `data/processed/hin/최종/` 전체 갱신.

| Phase | 내용 |
|---|---|
| **Phase 0** | 환경 설정, 소스 데이터 전체 로드 (`product_final_keywords.csv`, `product_ip_mapping.xlsx`, `product_promo_keywords.csv` 등) |
| **Phase 1 — 제품 노드** | POS·인스타 메타 집계 / `첫_등장일` (POS 우선 → 인스타 fallback) / `insta_mention_30d` 파생 / `키워드_final` 병합 → `product_nodes.parquet` 임시 저장 |
| **Phase 2 — IP 노드** | `product_ip_mapping.xlsx` IP별_키워드 시트 로드 → `ip_nodes.parquet` |
| **Phase 3 — 키워드 노드** | 제품·IP vocab 합집합 / 트렌드 키워드 매핑 / `인스타_첫_등장일` (트렌드만) → `keyword_nodes.parquet` |
| **Phase 3.5 — 키워드 EDA** | 키워드별 제품 수·IP 수·총등장수 집계 → `keyword_eda.csv` 저장 (검수 입력용) |
| **Phase 3.6 — 키워드 정규화** | `keyword_eda_final.csv`의 `제거/검토` 컬럼 파싱 → NOISE_REMOVE(O) / KW_RENAME_MAP(1개) / KW_SPLIT_MAP(2개+) 자동 구성 → `apply_norm` 적용 → `product_nodes`·`ip_nodes` 정규화 후 parquet 저장 / `keyword_nodes` 재구성 |
| **Phase 3.7 — 분포 확인** | 편의점별·IP별 키워드 수 분포 출력 / 키워드 0개 제품 목록 |
| **Phase 4 — 엣지: 제품-키워드** | `product_nodes.키워드_final` explode → `product_keyword_edges.parquet` |
| **Phase 4-Fill — 키워드 보충** | `keyword_fill_edges.parquet` 존재 시 under-tagged 제품 보충 엣지 병합 |
| **Phase 5 — 엣지: IP-키워드** | `ip_nodes.키워드_final` explode → `ip_keyword_edges.parquet` |
| **Phase 6 — 엣지: 트렌드-키워드** | `trend_keywords.추출_속성_final` explode → `trend_keyword_edges.parquet` |
| **Phase 7 — 엣지: 제품-IP** | `product_ip_mapping.xlsx` Sheet1 + `product_final_keywords.IP` 합집합 → `product_ip_edges.parquet` |
| **Phase 7.5 — 프로모션 원핫** | `product_promo_keywords.csv` → 18종 슬러그 매핑 → 원핫 피벗 → `product_nodes`에 `promo_*` 18컬럼 추가 후 `product_nodes.parquet` 갱신 저장 |
| **Phase 8 — 통계 요약** | 노드·엣지 수, 커버리지, 파일 목록 보고 |
| **Phase 9 — 제품 노드 패치** | `patch_product_nodes.py` — 동명 구버전 바코드 통합 + 키워드 합산 → `product_nodes.parquet` 최종 갱신 |
| **csv-export** | `product_nodes.parquet` → `product_nodes.csv` (Excel 직접 열기용, utf-8-sig) |
