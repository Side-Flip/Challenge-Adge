import random
import sys
from ga_utils import (
    inicializacion, fitness, elitismo,
    torneo, escx, inversion_mutation, supervivencia_seleccion
)

from hdfs_utils import read_population, write_population, read_cities

def mapper(island_id, hdfs_path, config):
    """
    Implementar la fase map del algoritmo genetico
    """
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