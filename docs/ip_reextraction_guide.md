# IP 속성 재추출 가이드

> 신규 IP(CU/GS 51개 등) 추가 시 어떤 데이터를 쓰고, 어디에 녹이는지 정리한 문서.  
> 매핑 대상 IP 목록은 [`docs/cu_gs_ip_mapping.md`](cu_gs_ip_mapping.md) 참조.

---

## 1. 전체 흐름 요약

```
[1] .txt 파일에 신규 IP 목록 작성
        ↓
[2] ip_attribute_extractor.py --rerun-file
    나무위키 + Naver 크롤링 → gemma4:26b LLM 추출
        ↓
    IP_속성_재추출.json  (체크포인트 방식, 중단 재시작 가능)
        ↓
[3] 00 노트북 Phase 3 Step 2
    IP_속성.json + IP_속성_재추출.json → IP_속성_통합.json (병합)
        ↓
[4] (수동 검수) ip_keyword_vocab_review.xlsx → ip_keyword_vocab_review_final.xlsx
        ↓
[5] 00 노트북 Phase 3 Step 4
    ip_keyword_vocab_review_final.xlsx 반영 → IP_키워드사전.json
        ↓
[6] (재추출 불가 IP 있을 경우) Step 4.5
    IP_속성_수동입력.json → IP_속성.json + IP_키워드사전.json 패치
        ↓
[7] 00 노트북 Block 13
    IP_키워드사전.json → ip_keywords.parquet  ← 최종 HIN 입력
```

---

## 2. 입력 데이터 (크롤링 소스)

| 소스 | 담당 함수 | 제한 |
|---|---|---|
| 나무위키 (`namu.wiki/w/{IP명}`) | `fetch_namuwiki_text()` | 최대 2,000자, 개요 섹션부터 추출 |
| Naver 블로그/뉴스 (`openapi.naver.com`) | `_fetch_naver_text()` | 최대 1,000자, 5건 snippet 합산 |

- 두 소스는 **항상 병합**하여 LLM에 전달 (`collect_enriched_text()`)
- Naver API 키는 `.env`의 `NAVER_CLIENT_ID` / `NAVER_CLIENT_SECRET` 필요
- 나무위키 없으면 Naver만, 둘 다 없으면 `"정보 없음"` fallback으로 LLM 실행

---

## 3. LLM 추출 스펙

| 항목 | 값 |
|---|---|
| 모델 | `gemma4:26b` (Ollama 로컬) |
| 엔드포인트 | `http://localhost:11434/api/generate` |
| 타임아웃 | 600초 (10분) |
| 온도 | 0.1 |
| 출력 속성 | `ip_type`, `target_age`, `signature_keywords` |

**`ip_type` 허용값**: `캐릭터·마스코트`, `K팝아이돌`, `배우·인플루언서`, `콘텐츠(영화·애니·예능)`, `브랜드·기타`  
**`target_age` 허용값**: `10대이하`, `2030`, `40대이상`, `전연령`  
**`signature_keywords`**: 단독 명사 5~8개 (정제 원칙 5단계 적용)

---

## 4. 파일 경로 정리

| 파일 | 역할 |
|---|---|
| `data/processed/IP_속성추출/IP.txt` | 최초 전체 배치 입력 목록 |
| `data/processed/IP_속성추출/IP_재추출.txt` | 1차 재추출 목록 |
| `data/processed/IP_속성추출/IP_재추출2.txt` | 2차 재추출 목록 |
| `data/processed/IP_속성추출/ip_attributes_checkpoint.json` | 전체 배치 체크포인트 |
| `data/processed/IP_속성추출/IP_속성.json` | 전체 배치 결과 |
| `data/processed/IP_속성추출/IP_속성_재추출.json` | `--rerun-file` 결과 (여기에 신규 IP 추가됨) |
| `data/processed/IP_속성추출/IP_속성_수동입력.json` | 재추출 불가 IP 수동 작성분 |
| `data/processed/IP_속성추출/IP_속성_통합.json` | Step 2 병합 결과 |
| `data/processed/IP_속성추출/IP_키워드사전.json` | Step 4 최종 사전 (`ip_name` + `최종키워드`) |
| `data/processed/ip_keywords.parquet` | **HIN 최종 입력** (`ip_name` + `키워드`) |

---

## 5. 신규 IP 추가 실행 절차

### Step 1 — txt 파일 작성

`data/processed/IP_속성추출/` 아래에 새 파일 생성 (예: `IP_신규_CU_GS.txt`).  
한 줄에 하나 또는 쉼표 구분:

```
T1
TXT
아이브
PLAVE
넷플릭스
오징어게임
...
```

### Step 2 — 재추출 실행

```bash
python src/data_builder/ip_attribute_extractor.py \
  --rerun-file data/processed/IP_속성추출/IP_신규_CU_GS.txt
```

- 결과는 `IP_속성_재추출.json`에 **누적 저장** (기존 성공 결과 보존)
- 중단 후 재실행하면 성공한 것은 건너뜀
- `signature_keywords` 비어 있으면 자동 2회 재시도 후 skip → 수동 입력 대상으로 처리

### Step 3 — 단일 IP 사전 테스트 (선택)

```bash
python src/data_builder/ip_attribute_extractor.py --keyword 오징어게임
```

### Step 4 — 00 노트북 Phase 3 실행

| 셀 | 작업 |
|---|---|
| **Phase 3 Step 2** | `IP_속성.json` + `IP_속성_재추출.json` → `IP_속성_통합.json` 병합 |
| **Phase 3 Step 3** (수동 검수용) | `ip_keyword_vocab_review.xlsx` export → 검수 후 `_final.xlsx` 저장 |
| **Phase 3 Step 4** | `ip_keyword_vocab_review_final.xlsx` 반영 → `IP_키워드사전.json` 생성 |
| **Step 4.5** | `IP_속성_수동입력.json` 있으면 패치 |
| **Block 13** | `IP_키워드사전.json` → `ip_keywords.parquet` 최종 저장 |

### Step 5 — 재추출 불가 IP 수동 처리

나무위키·Naver에도 정보가 없거나 LLM이 반복 실패한 IP는 `IP_속성_수동입력.json`에 직접 작성:

```json
[
  {
    "keyword": "달너새",
    "file_source": "IP_신규_CU_GS",
    "wiki_text_snippet": "",
    "text_source": "manual",
    "ip_type": "캐릭터·마스코트",
    "target_age": ["2030"],
    "signature_keywords": ["귀여움", "감성", "일상"]
  }
]
```

Step 4.5 셀이 이 파일을 읽어 `IP_속성.json` + `IP_키워드사전.json`에 자동 패치.

---

## 6. 최종 결과물이 HIN에 반영되는 위치

```
ip_keywords.parquet
  └─ ip_name (str)       ← 신규 CU/GS IP 51개 추가
  └─ 키워드  (list[str]) ← gemma4:26b 추출 + 수동 검수 반영

       ↓ (00 Block 12에서 ip_master_dataset.parquet에도 반영)

ip_master_dataset.parquet
  └─ ip_name
  └─ 최종키워드
  └─ 소속_커뮤니티  ← 트렌드 community_1hop_mapping에서 IP 소속 군집 매핑
```

> **주의**: `ip_keywords.parquet`만 업데이트해도 HIN 노드 피처 입력에는 충분.  
> `ip_master_dataset.parquet`까지 맞추려면 Block 12도 재실행 필요.

---

## 7. 주의사항

- **기존 IP는 건드리지 않음**: `--rerun-file`은 지정 파일의 IP만 추출, `IP_속성.json` 직접 수정 없음
- **병합 우선순위**: 같은 키워드가 `IP_속성.json`과 `IP_속성_재추출.json` 양쪽에 있으면 **재추출본 우선** (Step 2 로직)
- **`run_pipeline()` 정규화**: Block 13에서 `ip_keywords.parquet` 저장 시 키워드 전체에 `run_pipeline()` 재적용됨 — 수동 입력 키워드도 자동 정규화됨
- **Ollama 구동 필요**: 추출 실행 전 `ollama run gemma4:26b` 확인
