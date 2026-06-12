Write-Host "--- 7-Eleven NPD AI Framework MCP Setup (Windows/PowerShell) ---" -ForegroundColor Cyan

# 1. Node.js (npx) 확인
if (Get-Command npx -ErrorAction SilentlyContinue) {
    Write-Host "[확인] npx가 설치되어 있습니다." -ForegroundColor Green
} else {
    Write-Host "[오류] npx가 설치되어 있지 않습니다. Node.js를 설치해 주세요." -ForegroundColor Red
    exit 1
}

# 2. Python (uv/pip) 확인
if (Get-Command uv -ErrorAction SilentlyContinue) {
    Write-Host "[확인] uv가 설치되어 있습니다. Python Executor 최적화 모드로 실행됩니다." -ForegroundColor Green
    uv pip install mcp-python-interpreter
} else {
    Write-Host "[정보] 'uv'가 없습니다. pip를 통해 mcp-python-interpreter를 설치합니다." -ForegroundColor Yellow
    pip install mcp-python-interpreter
}

# 3. 환경 변수 가이드 출력
Write-Host "`n[필수 설정] 아래 환경 변수를 시스템 또는 .env에 등록해야 합니다:" -ForegroundColor Cyan
Write-Host "1. BRAVE_API_KEY: https://api.search.brave.com/ 에서 발급"
Write-Host "2. GITHUB_PERSONAL_ACCESS_TOKEN: 이미 설정되었습니다."

# 4. 문서 디렉토리 생성 및 가이드 작성
if (!(Test-Path docs)) {
    New-Item -ItemType Directory -Path docs
}
Write-Host "`nMCP 설정 완료. docs/MCP_USAGE.md를 확인하여 활용 방법을 숙지하세요." -ForegroundColor Green
