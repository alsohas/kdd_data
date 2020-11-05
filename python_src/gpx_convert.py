import ast
import os
import pandas as pd
from gpx_converter import Converter
from concurrent import futures
from tqdm import tqdm

wgs_folder = ''
gpx_folder = ''

def walk_folder():
    gps_folders = os.listdir(wgs_folder)
    for gps_folder in gps_folders:
        gps_folder_path = os.path.join(wgs_folder, gps_folder)
        trajectory_files = os.listdir(gps_folder_path)
        with tqdm(total=len(trajectory_files)) as pbar:
            futures = []
            with futures.ProcessPoolExecutor(max_workers=6) as executor:
                for trajectory_file in trajectory_files:
                    trajectory_file_path = os.path.join(gps_folder_path, trajectory_file)
                    future = executor.submit(process_file, trajectory_file_path)
                    futures.append(future)
                for future in concurrent.futures.as_completed(futures):
                    pbar.update(1)

def process_file(file_path):
    df = pd.read_csv(file_path, index_col='TRIP_ID')
    for trip_id, row in df.iterrows():
        polyline = ast.literal_eval(row['POLYLINE'])
        lon, lat = [i for i, j in polyline], [j for i, j in polyline]
        _df = pd.DataFrame({'x': lon, 'y': lat})
        out_path = os.path.join(gpx_folder, trip_id)
        out_path = f'{out_path}.gpx'
        if os.path.exists(out_path):
            print(f'Duplicate trip ID found {trip_id}')
        Converter.dataframe_to_gpx(_df, lats_colname='y', longs_colname='x', output_file=out_path)
 

def main():
    if not os.path.isdir(gpx_folder):
        os.mkdir(gpx_folder)
    walk_folder()

if __name__ == '__main__':
    main()
