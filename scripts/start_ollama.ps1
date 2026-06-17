# WSL Ollama 시작 스크립트
# 실행: .\scripts\start_ollama.ps1
# Ollama가 뜨면 이 창은 그대로 두고, 다른 터미널에서 스크립트 실행

$MODELS_PATH = "/mnt/c/Users/송정현/.ollama/models"
$CMD = "OLLAMA_MODELS=$MODELS_PATH OLLAMA_HOST=0.0.0.0:11435 ollama serve"

Write-Host "Ollama 시작 중 (WSL)..."
Write-Host "모델 경로: $MODELS_PATH"
Write-Host "포트: 11435"
Write-Host ""

wsl -- bash -c $CMD
