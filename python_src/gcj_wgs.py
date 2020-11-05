import ast
from coord_convert import transform
import pandas as pd
import os
import concurrent.futures


data_file = '/home/grad/aislam/didi/clean_gps/'
out_folder = '/home/grad/aislam/didi/wgs/'


def convert_polyline(polyline):
    polyline = ast.literal_eval(polyline)
    wgs_polyline = []
    for point in polyline:
        lng = point[0]
        lat = point[1]

        lng, lat = transform.gcj2wgs(lng, lat)
        wgs_polyline.append((lng, lat))
    return wgs_polyline

def convert_part_worker(part_path, part_file_name, folder):
    df = pd.read_csv(part_path)
    print(f'loaded: {part_path}')
    df['POLYLINE'] = df['POLYLINE'].map(lambda x: convert_polyline(x))
    out_path = os.path.join(out_folder, folder)
    try:
        if not os.path.exists(out_path):
            os.makedirs(out_path)
    except:
        pass
    out_file = os.path.join(out_path, part_file_name)
    df.to_csv(f'{out_file}.wgs')

def convert_parts(parts, folder_path, folder):
    with concurrent.futures.ProcessPoolExecutor(max_workers=15) as executor:      
        for part in parts:
            if '.part' not in part:
                continue
            part_path = os.path.join(folder_path, part)
            executor.submit(convert_part_worker, part_path, part, folder)

def convert_all():
    folders = os.listdir(data_file)
    for folder in folders:
        sub_folder = os.path.join(data_file, folder)
        parts = os.listdir(sub_folder)
        convert_parts(parts, sub_folder, folder)

if __name__=='__main__':
    convert_all()
