#!/home/hadoop/cluster_env/bin/python3

import sys, json
from utils.mapper_utils import mapper
import time

if __name__ == "__main__":
    start_time = time.time()

    config = json.load(open("config.json"))

    for line in sys.stdin:
        island_id = int(line.strip())
        mapper(island_id, "/", config)

    end_time = time.time()

    exc_time = end_time - start_time
    print(f"Tiempo de ejecución paralelo (MapReduce): {exc_time} segundos")