from ga_utils import fitness, elitismo
from hdfs_utils import write_elite, read_cities
import time

import time, json, sys

def _metric(name, value, **extra):
    # Línea única y parseable
    print("[METRIC] " + json.dumps({"name": name, "value": value, **extra}),
          file=sys.stderr, flush=True)

def reducer(island_id, individuals, config, hdfs_path='.'):
    t0 = time.perf_counter()

    tc0 = time.perf_counter()
    cities_path = config['cities_path']
    puntos = read_cities(cities_path)
    tc1 = time.perf_counter()
    _metric("reducer_read_cities_s", tc1 - tc0, island=island_id, n_cities=len(puntos))

    tf0 = time.perf_counter()
    fitnessnes = [fitness(ind, puntos) for ind in individuals]
    elite_size = config['elite_size']
    elite = elitismo(individuals, fitnessnes, elite_size)
    tf1 = time.perf_counter()
    _metric("reducer_select_elite_s", tf1 - tf0, island=island_id,
            n_individuals=len(individuals), elite_size=elite_size)

    tw0 = time.perf_counter()
    write_elite(island_id, elite, hdfs_path)
    tw1 = time.perf_counter()
    _metric("reducer_write_elite_s", tw1 - tw0, island=island_id, elite_size=len(elite))

    for ind in elite:
        print(f"{island_id}\t{ind}")

    t1 = time.perf_counter()
    _metric("reducer_total_s", t1 - t0, island=island_id, elite_size=len(elite))

import time

def reducer(island_id, individuals, config, hdfs_path='.'):
    start_time = time.time()  # Tiempo de inicio

    cities_path = config['cities_path']
    puntos = read_cities(cities_path)
    elite_size = config['elite_size']

    fitnessnes = [fitness(ind, puntos) for ind in individuals]
    elite = elitismo(individuals, fitnessnes, elite_size)

    write_elite(island_id, elite, hdfs_path)
    for ind in elite:
        print(f"{island_id}\t{ind}")

    end_time = time.time()  # Tiempo de fin
    elapsed_time = end_time - start_time  # Tiempo total
    print(f"[INFO] Tiempo de ejecución del reducer en isla {island_id}: {elapsed_time} segundos", file=sys.stderr)


"""
from ga_utils import fitness, elitismo
from hdfs_utils import write_elite, read_cities


def reducer(island_id, individuals, config, hdfs_path='.'):

    #Implementa la fase reduce del algoritmo genetico

    cities_path = config['cities_path']
    puntos = read_cities(cities_path)
    elite_size = config['elite_size']

    fitnessnes = [fitness(ind, puntos) for ind in individuals]
    
    elite = elitismo(individuals, fitnessnes, elite_size)
    
    write_elite(island_id, elite, hdfs_path)
    for ind in elite:
        print(f"{island_id}\t{ind}")
"""