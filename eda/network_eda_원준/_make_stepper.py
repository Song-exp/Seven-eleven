# -*- coding: utf-8 -*-
"""5단계 관통 스테퍼 — 5단계 표시 + 단계별 핵심 변화(엣지 중심). 성능 숫자 없음."""
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, Circle

plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ---- 데이터 ----
nums  = ['①', '②', '③', '④', '⑤']
names = ['베이스라인', '엣지 확장', 'IP 보완', '누수 제거', '최종 설계']
# 단계별 핵심 변화: (기호, line1, line2)
# 기호: '' 기본 / '+' 추가(그린) / '-' 제거(레드)
chg = [
    ('',  '기본 10종 엣지',     '고정 메타패스'),
    ('+', '유사상품 엣지 추가',  '상품-키워드-상품·상품-IP-상품'),
    ('+', 'IP 경유 간접 엣지',  'IP-IP 연결 복원'),
    ('-', '동반구매 엣지 제거',  '미래정보 누수 차단'),
    ('+', '키워드 궁합 엣지',    '멀티태스크 학습'),
]

GREEN = (0/255, 128/255, 97/255)
GRAY  = (200/255, 205/255, 210/255)
RED   = (192/255, 57/255, 43/255)
DARK  = (45/255, 49/255, 53/255)
MGRAY = (120/255, 125/255, 130/255)
def lerp(a, b, t): return tuple(a[i] + (b[i]-a[i])*t for i in range(3))
colors = [lerp(GRAY, GREEN, i/4) for i in range(5)]

# ---- 캔버스 ----
fig, ax = plt.subplots(figsize=(14, 4.3), dpi=200)
ax.set_xlim(0, 14); ax.set_ylim(0, 4.3); ax.axis('off')

xs = [1.75, 4.3, 6.85, 9.4, 11.95]
cy = 3.05
R  = 0.5

# 화살표
for i in range(4):
    ar = FancyArrowPatch((xs[i]+R+0.06, cy), (xs[i+1]-R-0.06, cy),
                         arrowstyle='-|>', mutation_scale=20,
                         lw=2.6, color=(155/255,160/255,165/255))
    ax.add_patch(ar)

for i in range(5):
    is_last = (i == 4)
    if is_last:
        ax.add_patch(Circle((xs[i], cy), R+0.1, facecolor='none',
                            edgecolor=GREEN, lw=2.2, ls=(0,(1,1)), zorder=2))
    ax.add_patch(Circle((xs[i], cy), R, facecolor=colors[i],
                        edgecolor='white', lw=2.3, zorder=3))
    tcol = 'white' if i >= 2 else (90/255,95/255,100/255)
    ax.text(xs[i], cy+0.02, nums[i], ha='center', va='center',
            fontsize=25, color=tcol, fontweight='bold', zorder=4)
    if is_last:
        ax.text(xs[i], cy+0.78, '★', ha='center', va='center',
                fontsize=15, color=GREEN, fontweight='bold')
    # 단계명
    lblcol = GREEN if is_last else DARK
    ax.text(xs[i], 2.12, names[i], ha='center', va='center',
            fontsize=15, color=lblcol, fontweight='bold')
    # 변화 line1 (기호 + 텍스트)
    sym, l1, l2 = chg[i]
    symcol = GREEN if sym == '+' else (RED if sym == '-' else MGRAY)
    prefix = {'+': '＋ ', '-': '－ ', '': ''}[sym]
    ax.text(xs[i], 1.55, prefix + l1, ha='center', va='center',
            fontsize=11, color=symcol, fontweight='bold')
    # 변화 line2 (디테일)
    ax.text(xs[i], 1.16, l2, ha='center', va='center',
            fontsize=9.8, color=MGRAY)

# 제목
ax.text(0.4, 3.98, '최종 모델까지 ', ha='left', va='center',
        fontsize=17, color=DARK, fontweight='bold')
ax.text(2.85, 3.98, '5단계', ha='left', va='center',
        fontsize=21, color=GREEN, fontweight='bold')
ax.text(3.95, 3.98, '의 설계 변화를 거쳐 완성', ha='left', va='center',
        fontsize=17, color=DARK, fontweight='bold')
ax.text(13.7, 3.98, '상세  ▶  부록', ha='right', va='center',
        fontsize=11.5, color=(155/255,160/255,165/255))

plt.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.02)
out = 'report_assets/stepper_5stage.png'
fig.savefig(out, dpi=200, bbox_inches='tight', facecolor='white')
print('saved', out)
