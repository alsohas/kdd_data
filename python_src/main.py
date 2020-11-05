import os
from collections import OrderedDict
from dask.diagnostics import ProgressBar
from coord_convert import transform
import dask
import dask.dataframe as dd

data_folder = '../scripts/gps/'
out_folder = '../clean_gps/'


def csvfy():
    if not os.path.isdir(out_folder):
        os.mkdirs(out_folder)  # make the output folder if it doesn't exist
    files = os.listdir(data_folder)
    cols = ['DRIVER_ID', 'TRIP_ID', 'TIMESTAMP', 'LONGITUDE', 'LATITUDE']

    for i, f in enumerate(files):
        csvfy_aux(f, i, cols)


def csvfy_aux(f, i, cols):
    df = dask.dataframe.read_csv(os.path.join(data_folder, f), names=cols)
    df = df.drop(columns=['DRIVER_ID'], axis=1)
    df = df.set_index('TIMESTAMP').persist()

    with ProgressBar():
        ndf = df.groupby('TRIP_ID').apply(lambda x: process_group(x.LONGITUDE, x.LATITUDE)).to_frame(
            name='POLYLINE').reset_index()

    with ProgressBar():
        ndf.to_csv(os.path.join(out_folder, f), index=False)


def process_group(lng, lat):
    lonlat = list(zip(lng, lat))

    # apply GCJ-02 -> WGS84 conversion
    lonlat = list(map(convert_latlong, lonlat))
    
    return lonlat


def convert_latlong(coordinate):
    '''
    Converts a single GCJ-02 coordinate to WGS84
    '''
    lng = coordinate[0]
    lat = coordinate[1]
    lng, lat = transform.gcj2wgs(lng, lat)
    return [lng, lat]


if __name__ == '__main__':
    csvfy()

