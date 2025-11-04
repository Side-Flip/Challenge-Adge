import random
from utils.ga_utils import (
    inicializacion, fitness, elitismo,
    torneo, escx, inversion_mutation, supervivencia_seleccion
)

from utils.hdfs_utils import read_population, write_population

def mapper(island_id, hdfs_path, config):
    """
    Implementar la fase map del algoritmo genetico
    """
    poblacion_size = config['population_size']
    num_ciudades = config['num_ciudades']
    elite_size = config['elite_size']
    migration_period = config['migration_period']
    puntos = config['points']

    poblacion = read_population(hdfs_path, island_id)
    if not poblacion:
        poblacion = inicializacion(poblacion_size, num_ciudades)
    else:
        elite_inividuals = config.get("elite_individuals", [])
        poblacion.extend(elite_inividuals)

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

    write_population(hdfs_path, island_id, poblacion)
    for ind in poblacion:
        print(f"{island_id}\t{ind}")