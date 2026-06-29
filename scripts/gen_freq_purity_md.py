"""키워드 빈도 × purity 그룹 검토표(md) 생성 — 판정마크 + purity 중앙값 포함."""
import pandas as pd

df = pd.read_csv('data/processed/hin/keyword_final.csv')
# ❌가짜(강등)는 전 문서에서 제외
df = df[df['판정'] != '가짜(강등)'].copy()

fb = [(3, 10, '니치(3-9)'), (10, 30, '중(10-29)'), (30, 100, '고(30-99)'), (100, 1e9, '메가(100+)')]
pb = [(0.50, 1.01, '강성공 ≥.50'), (0.35, 0.50, '성공기울기 .35–.50'),
      (0.18, 0.35, '중립대 .18–.35'), (0.10, 0.18, '실패기울기 .10–.18'), (-0.01, 0.10, '강실패 <.10')]

MARK = {'killer_확실': '✅', 'killer_불명확(노이즈)': '❓', '가짜(강등)': '❌', '조건부killer(IP)': '🔶',
        '진짜 악재(Δ<0)': '✅', '통계만(의심)': '⚠', '불명확': '❓'}
GRADE_MARK = {'T1': '🟢', 'T2': '🔵'}  # 매개 등급 (T1 무조건+ / T2 확장)

L = []
w = lambda s='': L.append(s)

w('# 키워드 빈도 × purity 그룹 검토표')
w()
w('> 모델 HIN-GNN(v2_sweepA) · 데이터 2025 세븐일레븐 NPD · 생성 2026-06-23')
w('> 소스: `data/processed/hin/keyword_final.csv` · 분석 진입점: `docs/keyword_classification_criteria.md`')
w('> **범위: killer · mine · 매개 3유형만** (중립 제외). **❌가짜(강등) 키워드 전부 제외**(killer 35개 강등분 제거).')
w()
w('---')
w()
w('## purity란')
w()
w('```')
w('Score_succ(k) = Σ_4경로 Σ_{성공제품 p} att(p,k)   # 성공 제품이 k에 보낸 어텐션 총합')
w('Score_fail(k) = Σ_4경로 Σ_{실패제품 p} att(p,k)')
w('purity(k)     = Score_succ / (Score_succ + Score_fail)')
w('base_rate     = 0.238   # 전체 제품의 23.8%만 성공')
w('```')
w()
w('- **"키워드 k로 흘러드는 전체 어텐션 중 성공 제품이 보낸 비중."** 어텐션 가중(모델 관점)이지 단순 카운트가 아님(카운트 버전은 WoE).')
w('- **기준선은 0.5가 아니라 base 0.238.** purity > 0.238 = 성공 특이, < 0.238 = 실패 특이.')
w('- **빈도와 무관한 비율값** → 빈도 다른 키워드끼리 같은 잣대로 비교 가능.')
w()
w('## WoE란 (Weight of Evidence)')
w()
w('purity의 **순수 카운트 버전**. 신용평가에서 온 증거 가중치.')
w()
w('```')
w('WoE(k) = ln( (supp_s / n_succ) / (supp_f / n_fail) )')
w('  supp_s = k 가진 성공 제품 수,  n_succ = 전체 성공 제품 수')
w('  supp_f = k 가진 실패 제품 수,  n_fail = 전체 실패 제품 수')
w('```')
w()
w('- **"성공 제품 중 k 보유 비율" vs "실패 제품 중 k 보유 비율"의 로그 비.** **기준선 = 0** (purity는 base 0.238, WoE는 0).')
w('- **WoE > 0 = 성공 신호, < 0 = 실패 신호, = 0 = 중립.** killer는 WoE>0, mine은 WoE<0.')
w('- **purity와 차이**: purity=어텐션 가중(모델 관점, rare 키워드 편향 O) / WoE=순수 카운트(통계 관점, 희소성 편향 X). '
  'killer 선정이 `purity≥0.50` **AND** `WoE>0`를 둘 다 요구하는 이유 = 어텐션과 실제 카운트가 *둘 다* 성공 쪽일 때만 통과(어텐션 아티팩트 방어).')
w('- **주의**: 빈도 1~2 소표본은 supp_f(또는 supp_s)=0이라 WoE가 ±13처럼 발산 → 절댓값 크다고 강한 신호 아님(빈도 floor가 거름).')
w()
w('## 표기 규약')
w()
w('각 칸 = `키워드(빈도 / purity 마크)`, purity 내림차순.')
w('- 첫 숫자 = **빈도**(직접 has_kw 제품 수), 둘째 = **purity**.')
w('- **마크** — killer/mine은 Δprob 개입검증 판정, 매개는 등급(grade):')
w()
w('| 마크 | killer | mine | 매개 |')
w('|---|---|---|---|')
w('| ✅ | 확실(Δ>+0.01) | 진짜 악재(Δ<−0.01) | — |')
w('| ❓ | 불명확(노이즈, ±0.01) | 불명확 | — |')
w('| 🔶 | 조건부(IP 있을 때만 +) | — | — |')
w('| ⚠ | — | 통계만(의심, Δ≥0) | — |')
w('| 🟢 | — | — | T1(무조건+ , delta_pos=1.0) |')
w('| 🔵 | — | — | T2(확장) |')
w()
w('> ❌가짜(강등)는 이 문서에서 전부 제외됨(killer 35개). 남은 killer는 ✅확실·❓불명확·🔶조건부만.')
w('> **중요**: 칸은 purity로 묶였지만 killer/mine 확정은 purity가 아니라 **Δprob(개입·재추론)** 부호로 갈린다 — 같은 칸에 ✅와 ❓ 공존 가능. 매개는 Δprob>0 보편리프트가 전제라 별도 판정 없이 등급만 표기.')
w()
w('## 3유형은 어떻게 뽑았나 — 3층 필터')
w()
w('학습된 HIN-GNN(v2_sweepA) **단일 추론 1회**에서 키워드별 어텐션·라벨을 집계(관찰)하고, 의심분만 **재추론**(개입)으로 인과 검증한다.')
w()
w('```')
w('1. 분류   (관찰, 단일추론)    purity·WoE·Score   → 성공/실패 특이성으로 1차 태깅')
w('2. 빈도보정 (관찰, 단일추론)    빈도 floor·Wilson  → 소표본 거품 제거(공주 n=1 등)')
w('3. Δprob검증 (개입, 재추론)    캐리어 고정+k 토글  → 유발/악재/보편 인과 확인')
w('```')
w()
w('- **killer·mine = 2축**(성공특이 ↔ 실패특이), **매개 = 직교축**(특이성과 무관한 보편 리프트).')
w('- **2단 등급**: T1(엄격 핵심) / T2(확장 후보). hub(무색무취 백본)는 내부 게이트라 출력 제외(= 중립).')
w('- 상세 공식: `docs/keyword_classification_criteria.md`.')
w()
w('## 빈도밴드별 purity 중앙값 / 평균  (❌가짜 제외 후)')
w()
w('> 각 칸 = `중앙값 / 평균`')
w()
w('| 빈도 | killer | mine | 매개 |')
w('|---|---|---|---|')
med = {}
for tag in ['killer', 'mine', '매개']:
    sub = df[(df.tag == tag) & (df['빈도'] >= 3)]
    med[tag] = {}
    for f0, f1, fl in fb:
        c = sub[(sub['빈도'] >= f0) & (sub['빈도'] < f1)]
        med[tag][fl] = ('%.3f / %.3f' % (c.purity.median(), c.purity.mean())) if len(c) else '—'
for f0, f1, fl in fb:
    w('| %s | %s | %s | %s |' % (fl, med['killer'][fl], med['mine'][fl], med['매개'][fl]))
w()
w('→ killer만 빈도 따라 단조 하락(base로 수렴 = 구체 라인→범용어). mine은 빈도 무관 저공(강건), 매개는 base 근처 평평(성공특이 아님).')
w()
w('## 축 기준')
w()
w('| 축 | 구간 |')
w('|---|---|')
w('| **빈도** | 니치 3–9 / 중 10–29 / 고 30–99 / 메가 100+ |')
w('| **purity** (base 0.238) | 강성공 ≥.50 / 성공기울기 .35–.50 / 중립대 .18–.35 / 실패기울기 .10–.18 / 강실패 <.10 |')
w()
w('> killer·mine·매개 전수.')
w()
w('## 이 리포트 읽는 법')
w()
w('1. **세로축(purity)으로 유형, 가로축(빈도)으로 추상화 수준**을 읽는다.')
w('   - killer = 성공특이(purity 높음). 빈도 낮을수록 *구체 라인/IP*(고창·빈츠), 높을수록 *범용 카테고리어*(빵·과자).')
w('   - mine = 실패특이(purity 낮음). **빈도 무관 강건** — 고빈도일수록 오히려 더 큰 구조적 실패(와인 300/0.05).')
w('   - 매개 = base 0.238 근처 + 어디든 +. **빈도 무관 평평**(특이성과 직교).')
w('2. **마크로 신뢰도**: ✅ 안심 · ❓ 노이즈(보류) · 🔶 IP 있을 때만 · ⚠ 의심 / 매개 🟢T1 우선 · 🔵T2 확장후보.')
w('3. **purity ≠ 처방효과.** purity는 "이 키워드 어텐션 중 성공제품 비중"(관찰)이고, 넣었을 때 오르나(Δprob, 개입)는 별개 축이다. 그래서 같은 칸에 ✅와 ❓가 공존한다.')
w('4. **실무 매핑**:')
w('   - 처방(넣어라): **killer 니치/중 × 강성공 ✅** (고창·하와이·미역·연유·페스츄리).')
w('   - 회피(빼라): **mine 전부**, 특히 고빈도(와인·다이어트·하이볼·제육·갈비).')
w('   - 범용 강화제: **매개 🟢T1** (즉석·저당·당충전·허쉬·고추).')
w('   - 보류·재검: **❓ 마크 + killer 고/메가**(빵·과자·바삭류).')
w('5. **해석 주의**: 시그니처는 추상 맛법칙이 아니라 **2025 제품 클러스터 성패의 학습**이다(고창=롯데 고창꿀고구마 라인). killer는 라인·브랜드 맥락과 함께, mine은 구조적이라 2026에도 유효, 매개는 트렌드라 분기 갱신.')
w()
w('---')
w()


def section(title, tag, note, mode='verdict'):
    """mode: 'verdict'(killer/mine 판정마크) | 'grade'(매개 등급마크)."""
    sub = df[(df.tag == tag) & (df['빈도'] >= 3)].copy()
    w('## %s  (n=%d)' % (title, len(sub)))
    w()
    if mode == 'verdict':
        vc = sub['판정'].value_counts()
        w('판정 분포: ' + ' · '.join('%s%s %d' % (MARK.get(k, ''), k, v) for k, v in vc.items()))
        w()
    elif mode == 'grade':
        vc = sub['grade'].value_counts()
        w('등급 분포: ' + ' · '.join('%s%s %d' % (GRADE_MARK.get(k, ''), k, v) for k, v in vc.items()))
        w()
    w(note)
    w()
    for f0, f1, fl in fb:
        band = sub[(sub['빈도'] >= f0) & (sub['빈도'] < f1)]
        if len(band) == 0:
            continue
        w('### 빈도 %s  (n=%d)' % (fl, len(band)))
        w()
        for p0, p1, pl in pb:
            cell = band[(band.purity >= p0) & (band.purity < p1)].sort_values('purity', ascending=False)
            if len(cell) == 0:
                continue
            parts = []
            for _, r in cell.iterrows():
                if mode == 'grade':
                    m = GRADE_MARK.get(str(r['grade']), '')
                else:
                    m = MARK.get(str(r['판정']), '')
                tail = (' ' + m) if m else ''
                parts.append('%s(%d/%.2f%s)' % (r.keyword, int(r['빈도']), r.purity, tail))
            w('- **purity %s** (n=%d): %s' % (pl, len(cell), ', '.join(parts)))
        w()


section('KILLER — 성공 특이', 'killer',
        '**선정 기준**: [관찰] 빈도≥3 ∧ supp_s≥3 ∧ purity≥0.50 ∧ Score_succ 상위25% ∧ WoE>0 → [검증] **Δprob>0**(임의 제품에 단독으로 넣어도 성공확률↑). '
        'T2는 Score_succ·att_lift로 확장(purity<0.5도 포함). Δ≤0이면 → 🔶IP조건부 또는 ❌가짜(이 문서서 제외).\n\n'
        '**빈도↑ 갈수록 purity가 base로 깎임 = 구체 성공 라인 → 범용 카테고리어로 변질.** (❌가짜 제외, ✅확실/❓불명확/🔶조건부만)\n'
        '- 니치×강성공 = 특정 성공 제품라인·IP·지역브랜딩(고창·빈츠·메타몽). 처방의 핵심.\n'
        '- 중×강성공 = 검증된 제법 속성(하와이·연유·페스츄리). 처방 신뢰도 1순위(✅비율 최고).\n'
        '- 고/메가×성공기울기 = 빵·초코·바삭·과자. purity가 base+0.14 약한 기울기 → "처방" 아닌 카테고리 시그니처. ✅❓ 혼재 = killer 자격 의심 칸.', mode='verdict')

section('MINE — 실패 특이', 'mine',
        '**선정 기준**: [관찰] 빈도≥3 ∧ supp_f≥3 ∧ purity≤0.15 ∧ Score_fail 상위25% ∧ WoE<0 → [검증] **Δprob<0**(넣으면 성공확률↓ = 악재). Δ≥0이면 ⚠통계만(의심). killer의 완전 대칭.\n\n'
        '**빈도↑ 갈수록 오히려 더 깨끗·강건 (killer와 정반대).** 와인(300/0.05)처럼 빈도 높은데 purity 극저 = 대형 구조적 실패 카테고리. ✅비율 90%.', mode='verdict')

section('매개 — 보편 증폭 (어디든 +)', '매개',
        '**선정 기준**: [관찰] 빈도≥5 ∧ 연결수 10~400(메가허브 제외) ∧ 단독성공률<0.45 ∧ killer/mine 아님 → [검증] **delta_pos≥0.7**(거의 전 캐리어에서 +) ∧ Δ평균>0.04. '
        'killer가 *성공 제품에 특이*하게 붙는 거라면, 매개는 *어디 붙여도* 올린다(특이성과 직교).\n\n'
        '혼자 약한데(단독↓) 어디 붙여도 성공확률↑. purity가 base 0.238 근처(성공특이 아님)로 모임 → killer처럼 빈도따라 안 깎이고 평평.\n'
        '- 🟢T1 = 무조건+(delta_pos=1.0) 핵심 증폭기 / 🔵T2 = 확장(delta_pos≥0.7).\n'
        '- 포맷(즉석·튀김·라면) · 기능(저당·당충전·고추) · 콜라보(허쉬·쿠앤크·캐릭터) · 재료(트러플·말차·국물).', mode='grade')

w('---')
w()
w('## 검토 시 가장 주목할 칸')
w()
w('1. **killer 고/메가 × 성공기울기** (빵·초코·바삭·과자·쿠키·삼각·돼지) → ✅❓ 혼재. killer 유지 vs 카테고리 시그니처 분리 vs 매개 이동 결정.')
w('2. **killer 니치/중 × 강성공** = 처방 shortlist 핵심(고창·하와이·미역·연유·페스츄리). ✅ 위주라 신뢰 높음.')
w('3. **매개 고빈도**(라면·말차·국물·저당·샐러드) vs **killer 고빈도**(빵·초코) → 둘 다 범용어인데 delta_pos(보편성)로 갈린 경계. 재배치 판단의 핵심 대조군.')

open('docs/keyword_frequency_purity_groups.md', 'w', encoding='utf-8').write('\n'.join(L))
print('rewritten with verdict marks. lines:', len(L))
