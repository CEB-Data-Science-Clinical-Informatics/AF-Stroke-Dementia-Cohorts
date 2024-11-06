import pandas as pd

af_mismatch = pd.read_csv('warehouse/src/temp/check_new_AF.csv')
stroke_mismatch = pd.read_csv('warehouse/src/temp/check_new_stroke.csv')


af_df = pd.read_parquet('G:/Shared drives/CEB data warehouse data back up/cohort_backup/2023Q4/AF/AF_warehouse_exported_22-02-2024.parquet.gzip')
stroke_df = pd.read_parquet('G:/Shared drives/CEB data warehouse data back up/cohort_backup/2023Q4/Stroke/Stroke_warehouse_exported_05-03-2024.parquet.gzip')





