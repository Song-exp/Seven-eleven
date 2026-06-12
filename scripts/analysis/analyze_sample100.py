import pandas as pd

df = pd.read_csv('data/processed/blog_with_keywords.csv', encoding='utf-8-sig')
sample = df.head(100).copy()
sample['본문길이'] = sample['본문내용'].fillna('').str.len()
filled = sample[sample['review_keywords'].fillna('') != ''].copy()
skipped = sample[sample['review_keywords'].fillna('') == '']

print(f'샘플 100건 중: 추출 {len(filled)}건 / skip {len(skipped)}건')
print()
print('[skip된 행의 본문길이]')
print(skipped[['검색어','본문길이']].to_string(index=False))
print()

filled['review_n'] = filled['review_keywords'].str.split(', ').str.len()
filled['hin_n'] = filled['hin_keywords'].fillna('').str.split(', ').apply(lambda xs: len([x for x in xs if x]))
print(f"review_keywords 개수: 평균 {filled['review_n'].mean():.1f}, 중앙 {filled['review_n'].median():.0f}")
print(f"  분포: {dict(filled['review_n'].value_counts().sort_index())}")
print(f"hin_keywords    개수: 평균 {filled['hin_n'].mean():.1f}, 중앙 {filled['hin_n'].median():.0f}")
print(f"  분포: {dict(filled['hin_n'].value_counts().sort_index())}")
print()

# 빈 hin_keywords 있는지
empty_hin = filled[filled['hin_keywords'].fillna('') == '']
print(f'review는 있지만 hin_keywords 빈 행: {len(empty_hin)}건')
print()

# 본문길이 vs 키워드 개수 상관
filled['본문길이'] = filled['본문내용'].fillna('').str.len()
print('[본문길이 구간별 평균 키워드 개수]')
bins = [0, 500, 2000, 5000, 10000, 1e9]
labels = ['<500','<2k','<5k','<10k','>=10k']
filled['bucket'] = pd.cut(filled['본문길이'], bins=bins, labels=labels, right=False)
print(filled.groupby('bucket', observed=True).agg(n=('review_n','count'), review_avg=('review_n','mean'), hin_avg=('hin_n','mean')).to_string())
