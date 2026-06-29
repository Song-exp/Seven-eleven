# 대시보드 배포 — Railway (단일 호스트: 프론트 + GNN 백엔드 + DeepSeek LLM)

> 링크 하나로 어디서든 접속. 백엔드(FastAPI)가 프론트(dashboard.html)까지 같이 서빙 → CORS·분리배포 불필요.
> LLM은 DeepSeek API(무 GPU), GNN(`/combo`·`/network`)은 컨테이너에서 torch CPU 추론.

## 아키텍처
```
사용자 ── https://<app>.up.railway.app ──▶ Railway 컨테이너 (Dockerfile)
                                            ├─ GET /              dashboard.html
                                            ├─ GET /config.js·/combo_data.js  (오프라인 캐시)
                                            ├─ POST /infer        DeepSeek API (속성 추론, 어휘제약)
                                            ├─ POST /network      GNN (serve.py, torch-free parquet)
                                            └─ POST /combo·/combo/pair  GNN (MDEngine, torch CPU)
```
프론트 `API_BASE`는 same-origin('')로 자동 해석(`dashboard.html`) → 같은 도메인으로 호출.

## 배포 산출물 (이 저장소에 포함)
| 파일 | 역할 |
|---|---|
| `Dockerfile` | python3.12-slim + requirements-deploy + 코드 + 번들을 런타임 경로로 COPY |
| `.dockerignore` | src/·Dashboard/·serving_assets/·requirements만 이미지에 포함 |
| `requirements-deploy.txt` | API 런타임 최소 의존성 (torch CPU·torch_geometric·fastapi…) |
| `railway.json` | Dockerfile 빌더 + `/health` 헬스체크 |
| `serving_assets/` | 백엔드가 읽는 모델·데이터 최소 번들(~2.7MB, git 추적). Dockerfile이 `experiments/results/v2_sweepA`·`data/processed[/hin]`로 되돌림 |
| `scripts/sync_serving_assets.py` | 재학습·재export·재빌드 후 번들 갱신 |

## 최초 배포 (1회)
1. 변경분 커밋·푸시 (serving_assets·Dockerfile 등 포함).
   ```bash
   git add -A && git commit -m "deploy: Railway Dockerfile + serving assets + DeepSeek LLM"
   git push
   ```
2. [railway.app](https://railway.app) → **New Project → Deploy from GitHub repo** → 이 저장소 선택.
   Dockerfile 자동 감지 → 빌드.
3. **Variables**(환경변수) 설정:
   - `DEEPSEEK_API_KEY` = (DeepSeek 키)
   - `LLM_PROVIDER` = `deepseek`
   - (PORT는 Railway가 자동 주입 — 설정 불필요)
4. **Settings → Networking → Generate Domain** → 공개 링크 생성.
5. 검증: `https://<app>.up.railway.app/health` → `{"status":"ok",...}` / 루트 접속 → 대시보드.

> ⚠ `.env`는 git에 안 올라감(시크릿). 로컬은 `.env`, 배포는 Railway Variables로 키 주입.

## 운영 워크플로우 — "로컬 수정 → 링크 반영"
- 자동 반영 아님. **푸시가 곧 배포** (Railway가 git 연동 자동 재빌드).
- 프론트/백엔드 코드 수정: 커밋·푸시 → 재배포.
- 캐시 재베이킹(`export_dashboard`/`export_combo_dashboard`): 생성된 `config.js`·`combo_data.js` 커밋·푸시.
- **모델 재학습·산출물 재생성 시**: `python -m scripts.sync_serving_assets` → 번들 갱신분 커밋·푸시.

## 확장·한계
- 단일 인스턴스. 대부분 트래픽은 캐시 히트(알려진 키워드=LLM 미사용, 같은 시드=`lru_cache`)라 부하 작음.
- 무료/슬립 티어는 비활성 후 첫 요청 콜드스타트(모델 로드 수 초). 상시 따뜻하게 하려면 유료 인스턴스.
- 트래픽이 커지면: 프론트를 Vercel/정적으로 분리(프론트는 `window.DASHBOARD_API`에 백엔드 URL 주입), 백엔드만 스케일.

## 로컬에서 컨테이너 검증(선택)
```bash
docker build -t npd-dash .
docker run -p 8000:8000 -e LLM_PROVIDER=deepseek -e DEEPSEEK_API_KEY=sk-... npd-dash
# http://localhost:8000
```
