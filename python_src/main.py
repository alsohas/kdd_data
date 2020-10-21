from tqdm import tqdm
import pandas as pd
import numpy as np
import os

data_folder = '../trajectories'
out_folder = '../cleaned_data'

def csvfy():
    files = os.listdir(data_folder)
    cols = ['DRIVER_ID', 'TRIP_ID', 'TIMESTAMP', 'LONGITUDE', 'LATITUDE']

    pbar = tqdm(total=len(files), desc=f'Processing files', position=0, leave=True)
    for f in files:
        df = pd.read_csv(os.path.join(data_folder, f), names=cols)
        df = df.drop(columns=['DRIVER_ID'], axis=1)
        df = df.sort_values(['TIMESTAMP'], ascending=True).groupby('TRIP_ID')

        dfs = pd.DataFrame(columns=['TRIP_ID', 'TIME_DELTA', 'POLYLINE'])

        _pbar = tqdm(total=len(df), desc='Processing groups', position=1, leave=True)
        count = 1
        for name, group in df:
            _df = group.reset_index()
            coords = []
            times = []
            for index, row in _df.iterrows():
                coords.append([row['LONGITUDE'], row['LATITUDE']])
                times.append(row['TIMESTAMP'])
            times = np.diff(np.array(times))
            times = sum(times) / max(len(times), 1)
            dfs.loc[len(dfs)] = [name, times, coords]
            count += 1
            if not count % 100:
                _pbar.update(100)

        _pbar.close()

        dfs.to_csv(os.path.join(out_folder, f'{f}.csv'), index=False)
        pbar.update(1)
    pbar.close()


if __name__ == '__main__':
    csvfy()
