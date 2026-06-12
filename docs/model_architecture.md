# 하이브리드 GNN 아키텍처 — 핵심 선행연구 및 적용 논리

## 개요

7-Eleven HIN(이기종 정보 네트워크)을 단일 모델로 해석하기 위해  
SOTA 논문 3편의 핵심 메커니즘을 융합한 하이브리드 GNN 아키텍처를 자체 설계.

---

## 1. KGAT (Knowledge Graph Attention Network)
> "정보 탐색의 병목을 해결하는 지식의 고속도로"

**문제**: 노드가 방대한 환경에서 Meta-path 수동 탐색 → 연산량 폭발(OOM)

**적용 메커니즘**: 인접행렬 거듭제곱($A^L$)을 통한 재귀적 메시지 패싱(Recursive Message Passing).  
경로를 일일이 탐색하는 대신, 행렬 곱셈으로 네트워크 전체 정보를 중심 노드로 한 번에 수렴.

**비즈니스 임팩트**:  
신상품(NPD) 노드 투입 시 1-Hop(직접 연결 속성)뿐 아니라 2~3-Hop 너머의  
'타 히트 상품 성공 맥락' + '외부 트렌드'까지 흡수 → Cold Start 극복.

---

## 2. HGT (Heterogeneous Graph Transformer)
> "데이터의 성질을 보존하는 이기종 스마트 필터"

**문제**: 일반 GNN(GraphSAGE, GCN)은 엣지 종류 무관하게 1/N 균등 혼합  
→ 유의미한 특징이 노이즈와 섞이는 오버스무딩(Over-smoothing) 발생.

**적용 메커니즘**: 타입별 동적 어텐션(Type-specific Dynamic Attention).  
엣지 종류(`[상품-TPO]`, `[상품-트렌드]`, `[상품-프로모션]`)마다  
독립적인 가중치 행렬($W_{type}$)을 부여하여 각각 학습.

**비즈니스 임팩트**:  
영수증 기반 '장바구니 동반 구매' 정보와 인스타 기반 '바이럴 텍스트' 정보를  
각 특성에 맞게 맞춤형 처리 → 노이즈 필터링 성능 극대화.

---

## 3. DiffMG (Differentiable Meta Graph Search)
> "AI가 스스로 성공 방정식을 깎아내는 미분 최적화 엔진"

**문제**: 수동 메타패스 의존 → 인간 편견 개입 + 블랙박스(Black Box) 현상.

**적용 메커니즘**:  
엣지 중요도(어텐션 가중치 $\alpha$)를 미분 가능한 연속 변수(Continuous Variable)로 설정.  
예측 성공 확률($\hat{y}$)과 실제 POS 매출($y$) 간 Loss를 역전파(Backpropagation)하여  
모델이 가중치를 스스로 업데이트.

**비즈니스 임팩트**:  
Softmax 기반 생존 경쟁 → 매출 기여 '꿀조합 경로' 가중치 ≈ 0.9 증폭,  
노이즈 경로 ≈ 0 도태.  
학습된 최종 가중치 맵 = **MD에게 제공하는 XAI 기반 기획 근거**로 직결.

---

## 세 논문의 역할 분담 요약

| 논문 | 역할 | 해결하는 문제 |
|---|---|---|
| KGAT | 다중 홉 정보 수렴 | Cold Start, OOM |
| HGT | 이기종 엣지 타입별 처리 | Over-smoothing, 노이즈 혼합 |
| DiffMG | 메타패스 자동 탐색 | 인간 편견, 블랙박스 |

---

## 구현 제약

- 프레임워크: PyTorch + 협의된 GNN 라이브러리 (미승인 패키지 `pip install` 금지)
- GPU 가속 필수: `device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')`
- 모델 설계·수정 시 로컬 규칙 우선 적용: `src/models/.claude-rules.md`
