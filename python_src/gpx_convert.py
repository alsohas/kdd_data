import ast
import os
import pandas as pd
from gpx_converter import Converter
from concurrent import futures
from tqdm import tqdm
import uuid

wgs_folder = '/home/grad/aislam/didi/wgs/'
gpx_folder = '/home/grad/aislam/didi/gpx_folder/'

def walk_folder():
    gps_folders = os.listdir(wgs_folder)
    for gps_folder in gps_folders:
        gps_folder_path = os.path.join(wgs_folder, gps_folder)
        trajectory_files = os.listdir(gps_folder_path)
        with futures.ProcessPoolExecutor(max_workers=8) as executor:
            for trajectory_file in trajectory_files:
                trajectory_file_path = os.path.join(gps_folder_path, trajectory_file)
                executor.submit(process_file, trajectory_file_path)

def process_file(file_path):
    df = pd.read_csv(file_path, index_col='TRIP_ID')
    unique_id = uuid.uuid4().hex
    _out_path = os.path.join(gpx_folder, unique_id)
    if not os.path.isdir(_out_path):
        os.mkdir(_out_path)

    count = 1
    with tqdm(total=len(df)) as pbar: 
        for trip_id, row in df.iterrows():
            polyline = ast.literal_eval(row['POLYLINE'])
            lon, lat = [i for i, j in polyline], [j for i, j in polyline]
            _df = pd.DataFrame({'x': lon, 'y': lat})
            
            out_path = os.path.join(_out_path, trip_id)
            out_path = f'{out_path}.gpx'
            
            if os.path.exists(out_path):
                print(f'Duplicate trip ID found {trip_id}')
            
            Converter.dataframe_to_gpx(_df, lats_colname='y', longs_colname='x', output_file=out_path)
            count += 1
            if not count % 500:
                pbar.update(500)
    pbar.update(500)
    print(f'Finished processing file {file_path}')

def main():
    if not os.path.isdir(gpx_folder):
        os.mkdir(gpx_folder)
    walk_folder()

if __name__ == '__main__':
    main()
