from ga_utils import fitness, elitismo
from hdfs_utils import write_elite, read_cities


def reducer(island_id, individuals, config, hdfs_path='.'):
    """
    Implementa la fase reduce del algoritmo genetico
    """
    cities_path = config['cities_path']
    puntos = read_cities(cities_path)
    elite_size = config['elite_size']

    fitnessnes = [fitness(ind, puntos) for ind in individuals]
    
    elite = elitismo(individuals, fitnessnes, elite_size)
    
    write_elite(island_id, elite, hdfs_path)
    for ind in elite:
        print(f"{island_id}\t{ind}")