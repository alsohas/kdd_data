import pathlib
from pathlib import Path
import json
from tqdm import tqdm
import ast
from leuvenmapmatching.matcher.distance import DistanceMatcher
from leuvenmapmatching.map.inmem import InMemMap
import pandas as pd
import osmnx as ox
import os
from concurrent.futures import ProcessPoolExecutor

wgs_folder = '../clean_gps/'
map_match_folder = '../map_matched/' 

bbox = {'north': 30.7308,
       'south': 30.6480,
       'west': 104.0350,
       'east': 104.1349}

file_name = 'chengu'

def download_graph():
    graph = ox.graph_from_bbox(
            north=bbox['north'], 
            south=bbox['south'], 
            east=bbox['east'], 
            west=bbox['west'],
            network_type='drive', truncate_by_edge=True, simplify=True)

    ox.save_graphml(graph, filepath=f'{file_name}')

def load_graph():
    graph = ox.load_graphml(file_name)
    return graph

def make_map(graph):
    graph_proj = ox.project_graph(graph)

    map_con = InMemMap("myosm", use_latlon=True, use_rtree=True, index_edges=True)

    nodes, _ = ox.graph_to_gdfs(graph_proj, nodes=True, edges=True)

    nodes_proj = nodes.to_crs("EPSG:3395")

    for nid, row in nodes_proj.iterrows():
        map_con.add_node(nid, (row['lat'], row['lon']))

    # adding edges using networkx graph
    for nid1, nid2, _ in graph.edges:
        map_con.add_edge(nid1, nid2)
    
    map_con.purge()
    return map_con

def init_map_match(map_con, infile_path, outfile_path):
    matcher = DistanceMatcher(map_con,
            max_dist=45,  # meter
            min_prob_norm=0.1,
                     obs_noise=25,   # meter
                     dist_noise=25,  # meter
                     non_emitting_states=True)

    df = pd.read_csv(infile_path)
    nodes = []
    prev_nodes = []
    all_nodes = []
    for _, row in df.iterrows():
        polyline = ast.literal_eval(row['POLYLINE'])
        prev_nodes = nodes
        for i in range(len(polyline)):
            polyline[i] = tuple(polyline[i][::-1])
        try:
            matcher.match(polyline)
            nodes = matcher.path_pred_onlynodes
            if set(nodes) == set(prev_nodes):
                continue
            all_nodes.append(nodes)
        except:
            continue
    with open(f'{outfile_path}', 'w') as outfile:
        json.dump(all_nodes, outfile, indent=2)
    print(f'Finished file: {infile_path}, found {len(all_nodes)} trips')

def match_runner(infile_path, outfile_path):
    graph = load_graph()
    map_con = make_map(graph)
    init_map_match(map_con, infile_path, outfile_path)


def main():
    if file_name not in os.listdir():
        download_graph()

    gps_folders = sorted(Path(wgs_folder).iterdir(), key=os.path.getmtime)
    with ProcessPoolExecutor(max_workers=8) as worker:
        for gps_folder in gps_folders:
            gps_folder_path = gps_folder
            gps_files = os.listdir(gps_folder_path)
            for gps_file in gps_files:
                gps_file_path = os.path.join(gps_folder_path, gps_file)
                out_file_path = os.path.join(map_match_folder, pathlib.PurePath(gps_folder_path).name)
                if not os.path.isdir(out_file_path):
                    os.makedirs(out_file_path)
                out_file_path = os.path.join(out_file_path, gps_file)
                worker.submit(match_runner, gps_file_path, out_file_path)

if __name__=='__main__':
    main()
