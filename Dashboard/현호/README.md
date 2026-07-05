# 네트워크 키워드 자동 추출 대시보드 실행 방법

## 1. 이 대시보드가 하는 일

이 대시보드는 상품 설명문을 입력하면 자동으로 키워드를 추출합니다.

- 기존 키워드: 현재 네트워크에 이미 등록된 키워드
- 신규 키워드: 현재 네트워크에는 없지만 자동 등록 기준을 통과한 키워드
- CSV 업로드 가능
- 결과 CSV 다운로드 가능

## 2. 압축을 풀었을 때 필요한 파일

압축을 풀면 아래 파일들이 같은 폴더 안에 있어야 합니다.

```text
dashboard.html
keyword_api_server.py
keyword_extraction_pipeline.py
network_keyword_dictionary.csv
stopword_dictionary.csv
english_keyword_allowlist.csv
sample_product_descriptions.csv
output/
  curated_keyword_mapping.csv
```

각 파일의 의미는 아래와 같습니다.

| 파일 | 의미 |
|---|---|
| `dashboard.html` | 브라우저에서 여는 대시보드 화면 |
| `keyword_api_server.py` | 대시보드와 Python 키워드 추출기를 연결하는 로컬 서버 |
| `keyword_extraction_pipeline.py` | 상품 설명문에서 키워드를 추출하는 핵심 코드 |
| `network_keyword_dictionary.csv` | 기존 네트워크 키워드 사전 |
| `stopword_dictionary.csv` | 불용어 사전 |
| `english_keyword_allowlist.csv` | 유지할 수 있는 영어 키워드 목록 |
| `output/curated_keyword_mapping.csv` | 검수된 키워드 매핑 사전 |
| `sample_product_descriptions.csv` | 테스트용 입력 CSV |

## 3. 처음 한 번만 설치할 것

Python이 설치되어 있어야 합니다.

터미널에서 아래 명령어를 실행합니다.

```bash
python -m pip install kiwipiepy
```

만약 `python` 명령어가 안 되면 아래처럼 실행합니다.

```bash
python3 -m pip install kiwipiepy
```

## 4. 실행 순서

### 4.1 터미널에서 압축 푼 폴더로 이동

예시:

```bash
cd "압축을 푼 폴더 경로"
```

예를 들어 폴더가 바탕화면에 있다면:

```bash
cd "~/Desktop/대시보드폴더"
```

### 4.2 키워드 추출 API 서버 실행

첫 번째 터미널에서 실행합니다.

```bash
python keyword_api_server.py
```

정상 실행되면 아래와 비슷한 문구가 나옵니다.

```text
시트2 키워드 API 실행 중: http://127.0.0.1:8010
종료하려면 Ctrl+C를 누르세요.
```

이 터미널은 끄지 말고 그대로 둡니다.

### 4.3 대시보드 화면 서버 실행

두 번째 터미널을 새로 열고, 같은 폴더로 이동합니다.

```bash
cd "압축을 푼 폴더 경로"
```

그 다음 아래 명령어를 실행합니다.

```bash
python -m http.server 8022
```

정상 실행되면 아래와 비슷한 문구가 나옵니다.

```text
Serving HTTP on :: port 8022
```

이 터미널도 끄지 말고 그대로 둡니다.

### 4.4 브라우저에서 대시보드 열기

브라우저 주소창에 아래 주소를 입력합니다.

```text
http://127.0.0.1:8022/dashboard.html
```

## 5. CSV 입력 형식

CSV 파일은 아래 3개 컬럼을 사용합니다.

```text
상품코드, 상품명, 상품 설명문
```

예시:

```csv
상품코드,상품명,상품 설명문
900001,라임모히토에이드,상큼하게 즐기는 라임모히토향 에이드 상품입니다.
900002,부라타토마토샐러드,부라타치즈와 토마토를 활용한 프리미엄 샐러드입니다.
```

테스트할 때는 함께 제공된 파일을 사용하면 됩니다.

```text
sample_product_descriptions.csv
```

## 6. 대시보드 사용 방법

### 6.1 단건 입력

1. `상품코드`를 입력합니다.
2. `상품명`을 입력합니다.
3. `상품 설명문`을 입력합니다.
4. `키워드 추출` 버튼을 누릅니다.

결과 영역에 기존 키워드와 신규 키워드가 분리되어 표시됩니다.

### 6.2 CSV 파일 업로드

1. `CSV 파일 업로드` 영역에서 CSV 파일을 선택합니다.
2. `업로드 파일 추출` 버튼을 누릅니다.
3. 상품별 추출 결과가 화면에 표시됩니다.
4. `추출 결과 CSV 다운로드` 버튼을 누르면 결과를 CSV로 저장할 수 있습니다.

## 7. 다운로드 결과 형식

다운로드되는 CSV는 상품 1개당 1행입니다.

```text
상품코드, 상품명, 원문 설명, 기존 키워드, 신규 키워드
```

키워드가 여러 개인 경우 `|`로 연결됩니다.

예시:

```csv
상품코드,상품명,원문 설명,기존 키워드,신규 키워드
900001,라임모히토에이드,상큼하게 즐기는 라임모히토향 에이드 상품입니다.,상큼|라임|향|에이드,라임모히토
```

## 8. 자주 생기는 문제

### 8.1 `ModuleNotFoundError: No module named 'kiwipiepy'`

Kiwi 패키지가 설치되지 않은 상태입니다.

```bash
python -m pip install kiwipiepy
```

또는:

```bash
python3 -m pip install kiwipiepy
```

### 8.2 `Address already in use`

이미 같은 서버가 실행 중이라는 뜻입니다.

- 기존 터미널에서 실행 중인 서버를 사용하면 됩니다.
- 새로 실행하고 싶으면 기존 서버 터미널에서 `Ctrl+C`를 눌러 종료한 뒤 다시 실행합니다.

### 8.3 대시보드는 열리는데 키워드 추출이 안 됨

대부분 `keyword_api_server.py`가 실행되지 않은 경우입니다.

첫 번째 터미널에서 아래 명령어가 실행 중인지 확인합니다.

```bash
python keyword_api_server.py
```

### 8.4 HTML 파일을 더블클릭해서 열었는데 CSV가 잘 안 됨

HTML 파일을 직접 더블클릭해서 열지 말고, 반드시 아래 방식으로 실행합니다.

```bash
python -m http.server 8022
```

그 다음 브라우저에서 아래 주소로 접속합니다.

```text
http://127.0.0.1:8022/dashboard.html
```

## 9. 종료 방법

실행 중인 터미널 2개에서 각각 `Ctrl+C`를 누르면 종료됩니다.

- `keyword_api_server.py` 실행 터미널
- `python -m http.server 8022` 실행 터미널
