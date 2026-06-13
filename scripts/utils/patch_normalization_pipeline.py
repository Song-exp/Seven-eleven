import json
import sys
import ast

sys.stdout.reconfigure(encoding='utf-8')

NB_PATH = 'eda/ipynb/03_b5_promo_eda.ipynb'

with open(NB_PATH, encoding='utf-8') as f:
    nb = json.load(f)

print(f"Before: {len(nb['cells'])} cells")

# ── 1. Cell 29에 정규화 셀 삽입 ───────────────────────────────────────
norm_cell_lines = [
    "import re\n",
    "\n",
    "def norm_event(name):\n",
    "    if not isinstance(name, str):\n",
    "        return ''\n",
    "    name = re.sub(r'^\\d{2}년\\s+\\d{1,2}월\\s+', '', name)\n",
    "    name = re.sub(r'^\\d{1,2}(~\\d{1,2})?월\\s+', '', name)\n",
    "    name = re.sub(r'\\s*\\(\\d+\\)\\s*$', '', name)\n",
    "    return name.strip()\n",
    "\n",
    "df_filtered = df_filtered.with_columns(\n",
    "    pl.col('행사명').map_elements(norm_event, return_dtype=pl.String).alias('행사명_norm')\n",
    ")\n",
    "print(f\"행사명_norm 컬럼 추가 완료\")\n",
    "print(f\"원본 행사명 고유값: {df_filtered['행사명'].n_unique():,}개\")\n",
    "print(f\"정규화 후 고유값:   {df_filtered['행사명_norm'].n_unique():,}개\")\n",
]

ast.parse(''.join(norm_cell_lines))

nb['cells'].insert(29, {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": norm_cell_lines,
})
print("Inserted normalization cell at index 29")

# 삽입 후 인덱스 확인
# old 31(duration code) → new 32
# old 32(markdown 7-2)  → new 33
# old 40(combined hmap) → new 41
# old 41(per-type hmap) → new 42

# ── 2. Cell 33에 정규화 기반 지속기간 분석 삽입 ────────────────────────
dur_norm_lines = [
    "# 정규화 행사명 기준 — 구간 합산 총 운영일수 분석\n",
    "from datetime import timedelta\n",
    "\n",
    "df_intervals = (\n",
    "    df_filtered\n",
    "    .filter(pl.col('행사종료일').dt.year() != 9999)\n",
    "    .group_by('행사명_norm')\n",
    "    .agg([\n",
    "        pl.col('행사개시일').alias('시작목록'),\n",
    "        pl.col('행사종료일').alias('종료목록'),\n",
    "        pl.col('중분류명').first().alias('중분류명'),\n",
    "        pl.col('프로모션_타입').first().alias('프로모션_타입'),\n",
    "    ])\n",
    "    .to_pandas()\n",
    ")\n",
    "\n",
    "def sum_interval_days(starts, ends):\n",
    "    intervals = sorted(zip(starts, ends))\n",
    "    cur_s, cur_e = intervals[0]\n",
    "    total = 0\n",
    "    for s, e in intervals[1:]:\n",
    "        if s <= cur_e + timedelta(days=1):\n",
    "            cur_e = max(cur_e, e)\n",
    "        else:\n",
    "            total += (cur_e - cur_s).days + 1\n",
    "            cur_s, cur_e = s, e\n",
    "    total += (cur_e - cur_s).days + 1\n",
    "    return total\n",
    "\n",
    "df_intervals['총운영일수'] = df_intervals.apply(\n",
    "    lambda r: sum_interval_days(r['시작목록'], r['종료목록']), axis=1\n",
    ")\n",
    "df_intervals['전체스팬'] = df_intervals.apply(\n",
    "    lambda r: (max(r['종료목록']) - min(r['시작목록'])).days + 1, axis=1\n",
    ")\n",
    "\n",
    "df_norm_dur = (\n",
    "    df_intervals[['행사명_norm', '중분류명', '프로모션_타입', '총운영일수', '전체스팬']]\n",
    "    .sort_values('총운영일수', ascending=False)\n",
    "    .reset_index(drop=True)\n",
    ")\n",
    "\n",
    "print(f\"정규화 후 고유 행사명: {len(df_norm_dur)}개\\n\")\n",
    "print(\"=== 총 운영일수 Top 30 (구간 합산, 공백 제외) ===\")\n",
    "print(df_norm_dur.head(30).to_string(index=False))\n",
    "\n",
    "fig, axes = plt.subplots(1, 2, figsize=(14, 5))\n",
    "axes[0].hist(df_norm_dur['총운영일수'], bins=30, color='steelblue', edgecolor='white')\n",
    "axes[0].set_title('총 운영일수 분포 (구간 합산, 공백 제외)')\n",
    "axes[0].set_xlabel('일수')\n",
    "axes[1].hist(df_norm_dur['전체스팬'], bins=30, color='coral', edgecolor='white')\n",
    "axes[1].set_title('전체 스팬 분포 (첫 개시일 ~ 마지막 종료일)')\n",
    "axes[1].set_xlabel('일수')\n",
    "plt.suptitle('정규화 행사명 기준 지속기간 분석', fontsize=12)\n",
    "plt.tight_layout()\n",
    "plt.show()\n",
]

ast.parse(''.join(dur_norm_lines))

# 현재 cell 33은 markdown(7-2 구독행사) → 그 앞(33)에 삽입
nb['cells'].insert(33, {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": dur_norm_lines,
})
print("Inserted normalized duration cell at index 33")

# 삽입 후: old 41 → new 42, old 42 → new 43

# ── 3. Cell 42, 43: norm_event 재정의 제거 → 행사명_norm 직접 사용 ─────

def strip_norm_event(lines):
    """norm_event 정의 블록 및 map_elements 호출 제거, 행사명_norm 직접 사용"""
    out = []
    skip = False
    i = 0
    while i < len(lines):
        line = lines[i]
        # norm_event 함수 정의 시작 → 함수 끝까지 스킵
        if line.startswith('def norm_event('):
            skip = True
        if skip:
            if i > 0 and not lines[i].startswith(' ') and not lines[i].startswith('\n') and not lines[i].startswith('def'):
                skip = False
            else:
                i += 1
                continue
        # map_elements로 행사명_norm 생성하는 블록 제거
        if 'map_elements(norm_event' in line or ("행사명_norm" in line and "map_elements" in line):
            # 멀티라인 with_columns 블록 스킵
            if 'df_p = df_p.with_columns(' in ''.join(lines[max(0,i-2):i+1]):
                i += 1
                continue
            if '.with_columns(' in line and 'map_elements' in ''.join(lines[i:i+4]):
                # skip until closing )
                depth = line.count('(') - line.count(')')
                i += 1
                while i < len(lines) and depth > 0:
                    depth += lines[i].count('(') - lines[i].count(')')
                    i += 1
                continue
        # 행사명_norm alias 생성 with_columns 블록 제거
        if 'alias(\'행사명_norm\')' in line or 'alias("행사명_norm")' in line:
            # 이미 df_filtered에 있으므로 이 with_columns 전체 스킵
            # 앞 라인들에서 with_columns( 찾아 제거
            pass
        out.append(line)
        i += 1
    return out

# 간단하게 처리: norm_event 관련 라인만 제거하고 group_by를 행사명_norm으로 교체
def clean_heatmap_cell(lines):
    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # import re 제거 (이미 위에서 import됨)
        if line.strip() == 'import re':
            i += 1
            continue
        # def norm_event ~ 함수 전체 제거
        if line.startswith('def norm_event('):
            i += 1
            while i < len(lines):
                l = lines[i]
                if l.startswith('    ') or l.strip() == '' or l.startswith('\n'):
                    i += 1
                else:
                    break
            continue
        # df_p.with_columns(norm_event map_elements) 블록 제거
        if '.map_elements(norm_event' in line:
            # 이전에 추가된 with_columns( 라인도 제거
            if new_lines and '.with_columns(' in new_lines[-1]:
                new_lines.pop()
            # 닫는 ) 까지 스킵
            depth = line.count('(') - line.count(')')
            i += 1
            while i < len(lines) and depth > 0:
                depth += lines[i].count('(') - lines[i].count(')')
                i += 1
            continue
        # group_by('행사명') → group_by('행사명_norm')
        line = line.replace("group_by('행사명')", "group_by('행사명_norm')")
        line = line.replace('group_by("행사명")', 'group_by("행사명_norm")')
        new_lines.append(line)
        i += 1
    return new_lines

for cell_idx, label in [(42, 'Cell42-combined'), (43, 'Cell43-pertype')]:
    orig = nb['cells'][cell_idx]['source']
    cleaned = clean_heatmap_cell(orig)
    try:
        ast.parse(''.join(cleaned))
        nb['cells'][cell_idx]['source'] = cleaned
        nb['cells'][cell_idx]['outputs'] = []
        nb['cells'][cell_idx]['execution_count'] = None
        print(f"{label} updated OK")
    except SyntaxError as e:
        print(f"{label} SyntaxError: {e}")
        print("--- cleaned source ---")
        print(''.join(cleaned)[:500])

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print(f"\nAfter: {len(nb['cells'])} cells")
print("Done.")
