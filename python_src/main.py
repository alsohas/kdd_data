import os
from dask.diagnostics import ProgressBar
import dask
import dask.dataframe as dd

data_folder = '../gps/'
out_folder = '../clean_gps/'


def csvfy():
    os.system(f'mkdir {out_folder}')  # make the output folder if it doesn't exist
    files = os.listdir(data_folder)
    cols = ['DRIVER_ID', 'TRIP_ID', 'TIMESTAMP', 'LONGITUDE', 'LATITUDE']

    for i, f in enumerate(files):
        csvfy_aux(f, i, cols)


def csvfy_aux(f, i, cols):
    with ProgressBar():
        df = dask.dataframe.read_csv(os.path.join(data_folder, f), names=cols)
    df = df.drop(columns=['DRIVER_ID'], axis=1)
    with ProgressBar():
        df = df.set_index('TIMESTAMP').persist()

    with ProgressBar():
        ndf = df.groupby('TRIP_ID').apply(lambda x: process_group(x.LONGITUDE, x.LATITUDE)).to_frame(
            name='POLYLINE').reset_index()

    with ProgressBar():
        ndf.to_csv(os.path.join(out_folder, f), index=False)


def process_group(lng, lat):
    lonlat = set(zip(lng, lat))
    return lonlat


if __name__ == '__main__':
    csvfy()
