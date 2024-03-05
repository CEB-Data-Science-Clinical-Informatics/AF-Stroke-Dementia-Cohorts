import polars as pl
from pathlib import Path
from tqdm import tqdm

class StandardFolder:
    def __init__(self, folder: str):
        self.folder = Path(r"{}".format(folder))
        
        self._folder = Path(r"{}".format(folder)).iterdir()
        for subfolder in self._folder:
            subfolder = subfolder.stem.lower()

            if 'other' in subfolder:
                continue

            if ('medic' in subfolder) | ('drug' in subfolder):
                self.meds = self.folder / subfolder
            elif ('lab' in subfolder):
                self.lab = self.folder / subfolder
            elif 'order' in subfolder:
                self.order = self.folder / subfolder
            elif ('diag' in subfolder) | ('dx' in subfolder):
                self.dx = self.folder / subfolder
            elif 'bill' in subfolder:
                self.bill = self.folder / subfolder
            elif 'oper' in subfolder:
                self.oper = self.folder / subfolder
            elif  subfolder.startswith('dea'):
                self.deaths = self.folder / subfolder
            elif ('vital' in subfolder) | ('vs' in subfolder):
                self.vs = self.folder / subfolder
            elif 'surgery' in subfolder:
                self.sx = self.folder / subfolder
            elif 'visit' in subfolder:
                self.visit = self.folder / subfolder
            elif 'demo' in subfolder:
                self.demo = self.folder / subfolder
            elif 'admi' in subfolder:
                self.admission = self.folder / subfolder
            elif 'anes' in subfolder:
                self.anes = self.folder / subfolder
            else:
                print(f'{subfolder} not included.')

    def csv_to_parquet_folder(self):
        # Get all .csv and .parquet.gzip files
        files= list(self.folder.rglob('*'))
        num_converted = 0
        for file in tqdm(files):
            new_file = file.with_suffix('.parquet.gzip')
            if file.suffix == '.csv':
                exists = new_file in files
                if exists:
                    continue
                # Read .csv and convert to .parquet.gzip
                # Try reading one column first, all as strings
                df = pl.read_csv(file, infer_schema_length=0, n_rows=1) # str
                if len(df.columns) == 1:
                    # Wrong separator, usually $$$
                    df = pl.read_csv(file, separator=r'$', infer_schema_length=0)
                    df = df.select(pl.col([i for i in df.columns if not (i.startswith('_')) | (len(i) == 0)]))
                else:
                    df = pl.read_csv(file, infer_schema_length=0)
                # Export as parquet
                df.write_parquet(new_file, compression='gzip')
                num_converted += 1

        print('Number of files converted: ', num_converted) 