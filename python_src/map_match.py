import json
from tqdm import tqdm
import ast
from leuvenmapmatching.matcher.distance import DistanceMatcher
from leuvenmapmatching.map.inmem import InMemMap
import pandas as pd
import osmnx as ox
import os
from concurrent.futures import ProcessPoolExecutor

wgs_folder = '/home/ai/Public/gps_20161130/'
map_match_folder = '/home/ai/Public/map_matched_v2/' 

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
    with tqdm(total=len(df)) as pbar:
        for _, row in df.iterrows():
            polyline = ast.literal_eval(row['POLYLINE'])
            prev_nodes = nodes
            for i in range(len(polyline)):
                polyline[i] = tuple(polyline[i][::-1])
            pbar.update(1)
            try:
                matcher.match(polyline)
                nodes = matcher.path_pred_onlynodes
                if set(nodes) == set(prev_nodes):
                    continue
                all_nodes.append(nodes)
            except:
                continue
        print(f'Got {len(all_nodes)} trips')
        with open(f'{outfile_path}', 'w') as outfile:
            json.dump(all_nodes, outfile, indent=2)

def match_runner(infile_path, outfile_path):
    graph = load_graph()
    map_con = make_map(graph)
    init_map_match(map_con, infile_path, outfile_path)


def main():
    if file_name not in os.listdir():
        download_graph()

    gps_files = os.listdir(wgs_folder)
    with ProcessPoolExecutor(max_workers=4) as exc:
        for f in gps_files:
            in_path = os.path.join(wgs_folder, f)
            outfile_path = os.path.join(map_match_folder, f)
            exc.submit(match_runner, in_path, outfile_path)

if __name__=='__main__':
    main()
