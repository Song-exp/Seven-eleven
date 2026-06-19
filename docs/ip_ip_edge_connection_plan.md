# IP-IP 엣지 연결 계획

## 현황 파악

| 항목 | 수치 |
|---|---|
| 기존 IP 노드 (`ip_nodes_final`) | 275개 |
| 신규 IP 노드 (`ip_extra_final.csv`) | 61개 |
| 생성될 IP-IP 엣지 | **66개** |
| 신규 IP-keyword 엣지 | **724개** |
| 기존에 없던 신규 keyword 노드 | **24개** |

---

## 그래프 구조 변화

### Before
```
제품 ──[product_ip]──▶ IP ──[ip_keyword]──▶ 키워드
```

### After
```
제품 ──[product_ip]──▶ IP₁ ──[ip_ip]──▶ IP₂ ──[ip_keyword]──▶ 키워드
                            └──[ip_keyword]──▶ 키워드
```

---

## 변경 파일 목록

| 파일 | 작업 | 변화 |
|---|---|---|
| `ip_nodes_final.parquet` | 신규 61개 행 append | 275 → **336개** |
| `ip_ip_edges_final.parquet` | **신규 생성** | 0 → **66개** |
| `ip_keyword_edges_final.parquet` | 신규 724개 행 append | 1,295 → **2,019개** |
| `keyword_nodes_final.parquet` | 신규 24개 키워드 추가 | 2,039 → **2,063개** |

---

## IP-IP 엣지 66개 전체 목록

> 기존 IP 키워드에 신규 IP명이 등장하는 경우 자동 생성.

| 기존 IP (src) | 신규 IP (tgt) |
|---|---|
| SK하이닉스 | 젠슨황 |
| T1 | LoL |
| T1 | 페이커 |
| TXT | 뿔바투 |
| FC모바일 | 넥슨 |
| FC모바일 | EA |
| FC모바일 | 월드컵 |
| 닌텐도 | 슈퍼마리오 |
| 라인프렌즈 | BT21 |
| 라인프렌즈 | 뉴진스 |
| 랏소베어 | 토이스토리 |
| 마이멜로디 | 산리오 |
| 마츠시게유타카 | 고독한미식가 |
| 맛삼춘 | 라이언 |
| 맛삼춘 | 카카오프렌즈 |
| 맛제일 | 비비고 |
| 먼작귀 | 치이카와 |
| 먼작귀 | 하치와레 |
| 먼작귀 | 우사기 |
| 몬델리즈 | 오레오 |
| 미연 | 여자아이들 |
| 박종혁 | 최강야구 |
| 배민스토어 | 배달의민족 |
| 브레드이발소 | 윌크 |
| 브롤스타즈 | 슈퍼셀 |
| 산리오캐릭터즈 | 폼폼푸린 |
| 산리오캐릭터즈 | 포차코 |
| 산리오캐릭터즈 | 헬로키티 |
| 시나모롤 | 산리오 |
| 엔하이픈 | 하이브 |
| 옐로우즈 | 오뚜기 |
| 오뎅 | 삼진어묵 |
| 오뎅 | 고래사 |
| 유해진 | 삼시세끼 |
| 이석원명장 | 성심당 |
| 이원일 | 냉장고를부탁해 |
| 이정후 | MLB |
| 지니스램프 | BTS |
| 최현석 | 냉장고를부탁해 |
| 카트라이더러쉬플러스 | 넥슨 |
| 카트라이더러쉬플러스 | 배찌 |
| 캐치티니핑 | 티니핑 |
| 캐치티니핑 | 하츄핑 |
| 케이팝데몬헌터스 | 헌트릭스 |
| 케이팝데몬헌터스 | 사자보이즈 |
| 케이팝데몬헌터스 | 레고 |
| 케플러 | 걸스플래닛999 |
| 케플러 | Mnet |
| 콕콕콕 | 오뚜기 |
| 쿠로미 | 산리오 |
| 클로즈유어아이즈 | JTBC |
| 토트넘 | 프리미어리그 |
| 토트넘 | 손흥민 |
| 트리플에스 | 모드하우스 |
| 페코짱 | 후지야 |
| 편스토랑 | KBS |
| 포켓몬스터 | 피카츄 |
| 플레이브 | 위버스 |
| 피스마이너스원 | 나이키 |
| 피크민 | 올리마 |
| 피크민 | 피크민블룸 |
| 핫휠 | 매텔 |
| 헬리녹스 | BTS |
| 홍콩제니쿠키 | 제니베이커리 |
| 혜자 | 김혜자 |
| 도라에몽 | 노진구 |

---

## 신규 keyword 노드 24개

> 기존 `keyword_nodes_final`에 없던 키워드. ip_extra_final 신규 IP들의 속성에서 발생.

`대체`, `메론`, `미트볼`, `볼`, `스티커`, `시골`, `야구`, `엽서`, `완구`, `전통주`,
`조합`, `주먹`, `찐빵`, `초콜릿`, `케이스`, `쿠폰`, `크루아상`, `키캡`, `텀블러`,
`토끼`, `패스츄리`, `팬`, `팬케이크`, `훠궈`

---

## 구현 순서

### Step 1 — ip_nodes_final 확장

```python
import pandas as pd, ast

existing = pd.read_parquet('data/processed/hin/ip_nodes_final.parquet')
extra = pd.read_csv('data/processed/hin/ip_extra_final.csv')
extra['키워드_final'] = extra['키워드_final'].apply(ast.literal_eval)

combined = pd.concat([existing, extra], ignore_index=True)
combined.to_parquet('data/processed/hin/ip_nodes_final.parquet', index=False)
# 275 → 336개
```

### Step 2 — ip_ip_edges_final 생성 (신규 파일)

```python
all_ip_set = set(combined['ip_name'])
new_ip_set  = set(extra['ip_name'])

ip_ip_rows = []
for _, row in existing.iterrows():           # 기존 IP 키워드에서만 탐색
    for kw in row['키워드_final']:
        if kw in new_ip_set:
            ip_ip_rows.append({'src_ip': row['ip_name'], 'tgt_ip': kw})

ip_ip_df = pd.DataFrame(ip_ip_rows).drop_duplicates()
ip_ip_df.to_parquet('data/processed/hin/ip_ip_edges_final.parquet', index=False)
# 66개
```

### Step 3 — ip_keyword_edges_final 확장

```python
existing_edges = pd.read_parquet('data/processed/hin/ip_keyword_edges_final.parquet')

new_rows = []
for _, row in extra.iterrows():
    for kw in row['키워드_final']:
        if kw not in all_ip_set:            # IP명은 ip_ip_edges로 처리, 제외
            new_rows.append({'ip_name': row['ip_name'], 'keyword': kw})

new_edges = pd.DataFrame(new_rows).drop_duplicates()
combined_edges = pd.concat([existing_edges, new_edges], ignore_index=True)
combined_edges.to_parquet('data/processed/hin/ip_keyword_edges_final.parquet', index=False)
# 1,295 → 2,019개
```

### Step 4 — keyword_nodes_final 확장

```python
existing_kw = pd.read_parquet('data/processed/hin/keyword_nodes_final.parquet')
existing_kw_set = set(existing_kw['keyword'])

new_kws = set(new_edges['keyword']) - existing_kw_set   # 24개

new_kw_rows = pd.DataFrame({
    'keyword': list(new_kws),
    'is_trend_keyword': False,
    '추출_속성': [[] for _ in new_kws],
    '인스타_첫_등장일': None,
})
combined_kw = pd.concat([existing_kw, new_kw_rows], ignore_index=True)
combined_kw.to_parquet('data/processed/hin/keyword_nodes_final.parquet', index=False)
# 2,039 → 2,063개
```

---

## HGT 모델 반영 사항

`build_hetero_data.py` 또는 서빙 로직에서 아래 엣지 타입 추가 필요.

```python
# 기존
data['ip', 'ip_keyword', 'keyword'].edge_index = ...

# 추가
data['ip', 'ip_ip', 'ip'].edge_index = ...     # ip_ip_edges_final
# 역방향도 추가 (메시지 패싱용)
data['ip', 'rev_ip_ip', 'ip'].edge_index = ...  # tgt→src 역방향
```

> 역방향 엣지를 추가하면 `산리오캐릭터즈 ← 헬로키티` 방향의 메시지도 흘러
> "이 캐릭터가 속한 IP는 무엇인가"를 학습할 수 있음.

---

## 주의사항

- `ip_extra_final.csv`의 신규 IP 키워드에 기존 IP명이 포함될 수 있음 → Step 3에서 `all_ip_set` 필터로 제외
- `오뚜기` IP에 엣지가 `옐로우즈`, `콕콕콕` 두 곳에서 들어옴 → 중복 허용 (멀티엣지) 또는 dedup 선택
- `냉장고를부탁해` IP는 `이원일`, `최현석` 두 곳에서 동시 참조 → 마찬가지로 멀티엣지 허용 권장
