# 대시보드 로컬 실행 런북

## 실행 순서

### Step 1. WSL Ollama 실행

WSL 터미널에서:

```bash
pkill ollama; OLLAMA_HOST=0.0.0.0:11435 OLLAMA_MODELS=/mnt/c/Users/송정현/.ollama/models ollama serve
```

### Step 2. FastAPI 백엔드 실행

PowerShell (프로젝트 루트):

```powershell
python -m uvicorn src.eval.api:app --port 8000
```

백엔드가 뜨면 백그라운드에서 Gemma 콜드 로드가 자동 시작됩니다.  
터미널에 `[warmup] Gemma 로드 완료` 가 뜨면 추론 준비 완료입니다.

### Step 3. 브라우저 접속

```
http://localhost:8000
```

---

## 구조 요약

| 역할 | 위치 | 포트 |
|---|---|---|
| Gemma LLM 서버 | WSL Ollama | 11435 |
| FastAPI 백엔드 + 정적 파일 | Windows Python | 8000 |
| 대시보드 UI | 브라우저 → localhost:8000 | — |

**왜 WSL Ollama인가?**  
Windows Ollama(11434)는 한글 사용자명 경로(`C:\Users\송정현`)에서 CLIP 모델 로드 실패.  
WSL에서 `OLLAMA_MODELS`를 Windows 경로로 지정해 우회.

---

## 변수별 고려사항

### WSL Ollama (Step 1)

**`Error: listen tcp 127.0.0.1:11434: bind: address already in use`**
- WSL 내부에 이미 Ollama가 떠 있음
- 원인: 명령어를 줄 나눠서 실행 → env 미전달 → 기본 11434로 바인드 시도
- 해결: 반드시 한 줄로 실행 (`pkill ollama; OLLAMA_HOST=... ollama serve`)

**WSL 재시작 후 모델 없음**
- WSL 껐다 켜면 Ollama 프로세스가 죽음
- Step 1 명령어를 다시 실행해야 함

**`ollama: command not found`**
- WSL에 Ollama가 설치되지 않은 상태
- WSL에서 `curl -fsSL https://ollama.com/install.sh | sh` 로 설치

**첫 추론 지연 (~190초)**
- `/mnt/c/` 경로가 9P 마운트라 9.6GB 모델 첫 로드가 느림
- Step 2 실행 후 `[warmup] Gemma 로드 완료` 를 기다렸다가 추론할 것
- 이후 30분간 메모리 유지 → 즉시 응답

**추론 속도 개선이 필요하면**
- 모델을 WSL 네이티브 파일시스템으로 복사하면 로드 수 초대로 단축:
  ```bash
  cp -r /mnt/c/Users/송정현/.ollama/models ~/ollama_models
  # 이후 실행 시 OLLAMA_MODELS=~/ollama_models 로 변경
  ```

---

### FastAPI 백엔드 (Step 2)

**`ModuleNotFoundError` 또는 import 오류**
- 가상환경 미활성화
- PowerShell에서 `.venv\Scripts\Activate.ps1` 실행 후 재시도
- WSL에서는 `.venv/bin/activate` (단, 백엔드는 Windows PowerShell에서 실행)

**`Address already in use` (포트 8000)**
- 이전 uvicorn 프로세스가 살아있음
- `Stop-Process -Name python -Force` 또는 작업 관리자에서 종료 후 재시작

**`[warmup] 실패`**
- WSL Ollama(11435)가 안 떠 있거나 모델 로드 실패
- Step 1이 정상 실행됐는지 확인

**파일 경로 오류 (dashboard.html 404)**
- 프로젝트 루트가 아닌 다른 디렉토리에서 실행
- 반드시 `7eleven_npd_framework/` 폴더에서 uvicorn 실행

---

### 브라우저 (Step 3)

**캐시된 검색어 vs. 신규 검색어**
- `Dashboard/config.js`에 사전 등록된 트렌드는 즉시 추론 (Gemma 호출 없음)
- 미등록 검색어는 Gemma 추론 → warmup 완료 후에만 정상 동작

**추론 결과 없음**
- Gemma warmup이 아직 안 끝난 경우 → 기다렸다가 재시도
- WSL Ollama가 꺼진 경우 → Step 1 재실행 후 warmup 대기

**네트워크 시각화가 안 그려짐**
- `/network` POST 호출 실패 → 백엔드 터미널 로그 확인
- 출발 속성(chip)을 하나 이상 선택해야 네트워크 생성됨

---

## 종료 방법

- **백엔드**: PowerShell에서 `Ctrl+C`
- **WSL Ollama**: WSL 터미널에서 `Ctrl+C` 또는 `pkill ollama`
- WSL 자체를 끄면 Ollama도 자동 종료됨
