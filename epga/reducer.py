import sys
import json
from collections import defaultdict
from ga_utils import fitness, elitismo
from hdfs_utils import write_elite, read_cities

def reducer(island_id, individuals, config, hdfs_path='.'):
    try:
        cities_path = config['cities_path']
        print(f"[DEBUG] Leyendo ciudades desde {cities_path}", file=sys.stderr)
        puntos = read_cities(cities_path)
        elite_size = config['elite_size']

        print(f"[DEBUG] Evaluando fitness de {len(individuals)} individuos", file=sys.stderr)
        fitnessnes = [fitness(ind, puntos) for ind in individuals]
        
        elite = elitismo(individuals, fitnessnes, elite_size)
        
        print(f"[DEBUG] Escribiendo elite de isla {island_id} en HDFS", file=sys.stderr)
        write_elite(island_id, elite, hdfs_path)
        
        for ind in elite:
            print(f"{island_id}\t{ind}")
    
    except Exception as e:
        print(f"[ERROR] Error en el reducer para isla {island_id}: {str(e)}", file=sys.stderr)
        sys.exit(1)
