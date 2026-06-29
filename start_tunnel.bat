@echo off
REM 고정 도메인 터널 — 서버 껐다 켜도 항상 같은 주소
REM 사전: uvicorn이 :8000에 떠 있어야 함 (python -m uvicorn src.eval.api:app --port 8000)
echo [ngrok] https://clerk-negation-stonework.ngrok-free.dev  ->  http://localhost:8000
C:\ngrok\ngrok.exe http 8000 --url=https://clerk-negation-stonework.ngrok-free.dev
