import random
import sys
from ga_utils import (
    inicializacion, fitness, elitismo,
    torneo, escx, inversion_mutation, supervivencia_seleccion
)

from hdfs_utils import read_population, write_population, read_cities


import time, json, sys

def _metric(name, value, **extra):
    # Línea única y parseable
    print("[METRIC] " + json.dumps({"name": name, "value": value, **extra}),
          file=sys.stderr, flush=True)
"""
def mapper(island_id, hdfs_path, config):

    Implementar la fase map del algoritmo genetico
    poblacion_size = config['population_size']
    elite_size = config['elite_size']
    migration_period = config['migration_period']
    archivo_puntos = config['cities_path']

    puntos = read_cities(archivo_puntos)

    if not puntos or len(puntos) == 0:
        num_ciudades = 20
        print(f"[DEBUG] No se encontraron puntos en HDFS, generando {num_ciudades} ciudades aleatorias", file=sys.stderr)
        puntos = [(random.uniform(0, 100), random.uniform(0, 100)) for _ in range(num_ciudades)]

    num_ciudades = len(puntos)

    poblacion = read_population(hdfs_path, island_id)
    
    if not poblacion:
        poblacion = inicializacion(poblacion_size, num_ciudades)
    else:
        elite_inividuals = config.get("elite_individuals", [])
        poblacion.extend(elite_inividuals)

    #Proceso de evolucion por cada isla

    for _ in range(migration_period):
        fitnessnes = [fitness(ind, puntos) for ind in poblacion]
        elite = elitismo(poblacion, fitnessnes, elite_size)
        offspring = [] 

        for _ in range(poblacion_size - elite_size):
            p1 = torneo(poblacion, fitnessnes)
            p2 = torneo(poblacion, fitnessnes)
            child = escx(p1, p2)
            child = inversion_mutation(child)
            offspring.append(child)

        fitness_offspring = [fitness(ind, puntos) for ind in offspring]
        poblacion = supervivencia_seleccion(
            poblacion, fitnessnes, offspring, fitness_offspring, poblacion_size
        )

        poblacion.extend(elite)
        if not poblacion:
            poblacion = elite[:]

    write_population(hdfs_path, island_id, poblacion)

    print(f"[DEBUG] {island_id}: número de puntos = {len(puntos)}", file=sys.stderr)

    for ind in poblacion:
        print(f"{island_id}\t{ind}")
"""

def mapper(island_id, hdfs_path, config):
    t0 = time.perf_counter()
    _metric("mapper_start", 0, island=island_id)

    poblacion_size = config['population_size']
    elite_size = config['elite_size']
    migration_period = config['migration_period']
    archivo_puntos = config['cities_path']

    tc0 = time.perf_counter()
    puntos = read_cities(archivo_puntos)
    if not puntos or len(puntos) == 0:
        num_ciudades = 20
        print(f"[DEBUG] No se encontraron puntos en HDFS, generando {num_ciudades} ciudades aleatorias", file=sys.stderr)
        puntos = [(random.uniform(0, 100), random.uniform(0, 100)) for _ in range(num_ciudades)]
    tc1 = time.perf_counter()
    _metric("mapper_read_cities_s", tc1 - tc0, island=island_id, n_cities=len(puntos))

    tp0 = time.perf_counter()
    poblacion = read_population(hdfs_path, island_id)
    tp1 = time.perf_counter()
    _metric("mapper_read_population_s", tp1 - tp0, island=island_id,
            n_individuals=(len(poblacion) if poblacion else 0))

    if not poblacion:
        poblacion = inicializacion(poblacion_size, len(puntos))
    else:
        elite_inividuals = config.get("elite_individuals", [])
        poblacion.extend(elite_inividuals)

    tevo0 = time.perf_counter()
    for gen in range(migration_period):
        g0 = time.perf_counter()
        fitnessnes = [fitness(ind, puntos) for ind in poblacion]
        elite = elitismo(poblacion, fitnessnes, elite_size)
        offspring = []

        for _ in range(poblacion_size - elite_size):
            p1 = torneo(poblacion, fitnessnes)
            p2 = torneo(poblacion, fitnessnes)
            child = escx(p1, p2)
            child = inversion_mutation(child)
            offspring.append(child)

        fitness_offspring = [fitness(ind, puntos) for ind in offspring]
        poblacion = supervivencia_seleccion(
            poblacion, fitnessnes, offspring, fitness_offspring, poblacion_size
        )
        poblacion.extend(elite)
        if not poblacion:
            poblacion = elite[:]

        g1 = time.perf_counter()
        # Si quieres ver cada generación
        _metric("mapper_generation_s", g1 - g0, island=island_id, gen=gen, pop=len(poblacion))
    tevo1 = time.perf_counter()
    _metric("mapper_evolution_total_s", tevo1 - tevo0, island=island_id, gens=migration_period)

    tw0 = time.perf_counter()
    write_population(hdfs_path, island_id, poblacion)
    tw1 = time.perf_counter()
    _metric("mapper_write_population_s", tw1 - tw0, island=island_id, pop=len(poblacion))

    print(f"[DEBUG] {island_id}: número de puntos = {len(puntos)}", file=sys.stderr)

    for ind in poblacion:
        print(f"{island_id}\t{ind}")

    t1 = time.perf_counter()
    _metric("mapper_total_s", t1 - t0, island=island_id, pop=len(poblacion))
