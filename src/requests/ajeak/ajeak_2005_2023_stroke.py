import polars as pl

df_af = pl.read_csv(r'D:\Prut\Warehouses\output\Dec23\n\requests\2005-2009\af_2005_2009.csv')
df_st_1 = pl.read_csv(r'D:\Prut\Warehouses\output\Dec23\n\requests\2005-2009\stroke_2005_2009.csv')

df_af_2 = pl.read_csv(r'D:\Prut\Warehouses\output\Dec23\n\AF\af_n_21032024.csv')
df_st_2 = pl.read_csv(r'D:\Prut\Warehouses\output\Dec23\n\Stroke\stroke_n_updated_05032024.csv')

req_af = pl.read_csv(r'D:\Prut\Warehouses\output\Dec23\n\requests\check_new_AF.csv')
req_st = pl.read_csv(r'D:\Prut\Warehouses\output\Dec23\n\requests\check_new_stroke.csv')

# Request 1: Discrepancies

set(req_af['ENC_HN']) & set(df_af_2['ENC_HN'])

set(req_st['ENC_HN']) & set(df_st_2['ENC_HN'])

# Request 2: all stroke hn
pl.DataFrame([list(set(df_st_1.to_series().to_list() + df_st_2.to_series().to_list()))]).transpose().write_csv(r'D:\Prut\Warehouses\output\Dec23\n\requests\request_29032024.csv')

