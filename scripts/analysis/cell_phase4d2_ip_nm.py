# Phase 4D-2: CU/GS IP_NM 컬럼 추가
# Cell 46 (Phase 4D-1) 바로 다음에 붙여넣기
# 키: url + '||' + 원본명 (Cell 46과 동일)

_CU_IP_XL = os.path.join(_EXCEL_DIR, 'CU_instagram_ip.xlsx')
_GS_IP_XL = os.path.join(_EXCEL_DIR, 'GS_instagram_IP.xlsx')

_cu_ip = pd.read_excel(_CU_IP_XL)
_gs_ip = pd.read_excel(_GS_IP_XL)

# GS 편의점 null 5개 보정
_gs_ip['편의점'] = _gs_ip['편의점'].fillna('GS25')

# 컬럼명 통일: ip / IP → IP_NM
_cu_ip = _cu_ip.rename(columns={'ip': 'IP_NM'})
_gs_ip = _gs_ip.rename(columns={'IP': 'IP_NM'})

# 키 생성 (Cell 46과 동일 패턴)
_cu_ip['_key'] = _cu_ip['url'].fillna('') + '||' + _cu_ip['원본명'].astype(str)
_gs_ip['_key'] = _gs_ip['url'].fillna('') + '||' + _gs_ip['원본명'].astype(str)

# IP_NM 매핑 딕셔너리 (채워진 것만)
_ip_map = pd.concat([
    _cu_ip[_cu_ip['IP_NM'].notna()].drop_duplicates('_key').set_index('_key')[['IP_NM']],
    _gs_ip[_gs_ip['IP_NM'].notna()].drop_duplicates('_key').set_index('_key')[['IP_NM']],
])

# parquet 로드 → IP_NM merge → 저장
_ds1 = pd.read_parquet(_OUT_DS1)
_ds1['_key'] = _ds1['url'].fillna('') + '||' + _ds1['원본명'].astype(str)
_ds1 = _ds1.merge(_ip_map.reset_index(), on='_key', how='left').drop(columns=['_key'])
_ds1.to_parquet(_OUT_DS1, index=False, engine='pyarrow')

print('[IP_NM 추가]')
for _cvs in ['CU', 'GS25', '세븐일레븐']:
    _m = _ds1['편의점명'] == _cvs
    _filled = _ds1.loc[_m, 'IP_NM'].notna().sum()
    print(f'  {_cvs}: {_m.sum()}행 중 IP_NM 채워짐 {_filled}개 / NaN {_m.sum()-_filled}개')

del _cu_ip, _gs_ip, _ip_map
