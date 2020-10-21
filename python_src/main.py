import concurrent.futures
import sys

import pandas as pd
import numpy as np
import os

from tqdm import tqdm

data_folder = '../scripts/gps/'
out_folder = '../clean_gps/'


def csvfy():
    os.system(f'mkdir {out_folder}')  # make the output folder if it doesn't exist
    files = os.listdir(data_folder)
    cols = ['DRIVER_ID', 'TRIP_ID', 'TIMESTAMP', 'LONGITUDE', 'LATITUDE']

    arg_tuples = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        for i, f in enumerate(files):
            future = executor.submit(csvfy_aux, f, i, cols)


def csvfy_aux(f, i, cols):
    df = pd.read_csv(os.path.join(data_folder, f), names=cols)
    df = df.drop(columns=['DRIVER_ID'], axis=1)
    df = df.sort_values(['TIMESTAMP'], ascending=True).groupby('TRIP_ID')

    dfs = pd.DataFrame(columns=['TRIP_ID', 'TIME_DELTA', 'POLYLINE'])

    pbar = tqdm(total=len(df), desc='Processing groups', position=i+1, leave=True)
    count = 1
    update_interval = 1000
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
        if not count % update_interval:
            pbar.update(update_interval)
            sys.stdout.flush()
    pbar.update((count % update_interval) + 1)
    sys.stdout.flush()
    pbar.close()

    dfs.to_csv(os.path.join(out_folder, f'{f}.csv'), index=False)


if __name__ == '__main__':
    csvfy()
