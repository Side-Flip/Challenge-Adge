from utils.ga_utils import fitness, elitismo
from utils.hdfs_utils import write_elite


def reducer(island_id, individuals, config, hdfs_path='.'):
    """
    Implementa la fase reduce del algoritmo genetico
    """
    puntos = config['points']
    elite_size = config['elite_size']
    fitnessnes = [fitness(ind, puntos) for ind in individuals]
    elite = elitismo(individuals, fitnessnes, elite_size)
    write_elite(island_id, elite, hdfs_path)
    for ind in elite:
        print(f"{island_id}\t{ind}")