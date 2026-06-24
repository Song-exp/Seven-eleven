# 7-Eleven NPD 대시보드 백엔드 (FastAPI: GNN /combo·/network + DeepSeek /infer + 프론트 서빙)
# Railway 등 상시 컨테이너 호스트용. GPU 불필요(LLM은 DeepSeek API).
FROM python:3.12-slim

WORKDIR /app

# torch(cpu)·pyarrow 휠은 manylinux라 시스템 패키지 빌드 불필요 → slim 그대로 사용

# 의존성 먼저 (레이어 캐시)
COPY requirements-deploy.txt .
RUN pip install --no-cache-dir -r requirements-deploy.txt

# 애플리케이션 코드 + 프론트
COPY src/ ./src/
COPY Dashboard/ ./Dashboard/

# 모델·데이터 산출물(serving_assets, git 추적됨)을 코드가 읽는 런타임 경로로 복사
COPY serving_assets/processed/ ./data/processed/
COPY serving_assets/hin/       ./data/processed/hin/
COPY serving_assets/model/     ./experiments/results/v2_sweepA/

ENV PYTHONUNBUFFERED=1
# LLM_PROVIDER / DEEPSEEK_API_KEY 는 Railway 환경변수로 주입 (이미지에 넣지 않음)

EXPOSE 8000
# Railway가 $PORT 주입 → 셸 폼으로 확장 (없으면 8000)
CMD uvicorn src.eval.api:app --host 0.0.0.0 --port ${PORT:-8000}
