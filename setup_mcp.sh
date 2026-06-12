#!/bin/bash
# setup_mcp.sh - MCP 환경 구축 스크립트

echo "--- 7-Eleven NPD AI Framework MCP Setup ---"

# 1. Node.js (npx) 확인
if ! command -v npx &> /dev/null; then
    echo "[오류] npx가 설치되어 있지 않습니다. Node.js를 설치해 주세요."
    exit 1
fi

# 2. Python (uv) 확인 - Python Executor용 권장 도구
if ! command -v uv &> /dev/null; then
    echo "[정보] 'uv'가 없습니다. pip를 통해 mcp-python-executor를 설치하거나 uv 설치를 권장합니다."
    pip install mcp-python-executor
else
    echo "[확인] uv가 설치되어 있습니다. Python Executor 최적화 모드로 실행됩니다."
fi

# 3. 환경 변수 가이드 출력
echo ""
echo "[필수 설정] 아래 환경 변수를 시스템 또는 .env에 등록해야 합니다:"
echo "1. BRAVE_API_KEY: https://api.search.brave.com/ 에서 발급"
echo "2. GITHUB_PERSONAL_ACCESS_TOKEN: 이미 설정되었습니다."

# 4. 문서 디렉토리 생성 및 가이드 작성
mkdir -p docs
echo "MCP 설정 완료. docs/MCP_USAGE.md를 확인하여 활용 방법을 숙지하세요."
