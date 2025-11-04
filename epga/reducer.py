#!/home/hadoop/cluster_env/bin/python3

import sys, json
from utils.reducer_utils import reducer

if __name__ == "__main__":
    config = json.load(open("config.json"))
    current_island = None
    individuals = []

    for line in sys.stdin:
        island, ind_str = line.strip().split('\t', 1)
        island = int(island)
        individual = eval(ind_str)

        if current_island is None:
            current_island = island

        if island != current_island:
            reducer(current_island, individuals, config, "hdfs_sim/")
            current_island = island
            individuals = []

        individuals.append(individual)

    if individuals:
        reducer(current_island, individuals, config, "hdfs_sim/")
