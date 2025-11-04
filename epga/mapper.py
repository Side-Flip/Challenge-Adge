#!/home/hadoop/cluster_env/bin/python3

import sys, json
from utils.mapper_utils import mapper

if __name__ == "__main__":
    config = json.load(open("config.json"))
    for line in sys.stdin:
        island_id = int(line.strip())
        mapper(island_id, "hdfs_sim/", config)