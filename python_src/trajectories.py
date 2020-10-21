import os
from tqdm import tqdm

def main():
    _trajectory_files = os.listdir('.')
    trajectory_files = []
    for index, f_name in enumerate(_trajectory_files):
        if 'gps' not in f_name:
            continue
        f_path = os.path.join('.', f_name)
        trajectory_files.append(f_path)
   
    print('\n'.join(trajectory_files)) 
    with open('./trajectories.txt', 'w') as out_file:
        bar = tqdm(total=len(trajectory_files), desc=f'Combining files')
        for trajectory_file in trajectory_files:
            with open(trajectory_file, 'r', encoding='utf-8') as in_file:
                for i, line in enumerate(in_file):
                    line = line.split(',')[1:]
                    line = ','.join(line)
                    out_file.write(line)
            bar.update(1)
        bar.close()

if __name__ == '__main__':
    # main()
    print('do not use this to download data, use curl')
