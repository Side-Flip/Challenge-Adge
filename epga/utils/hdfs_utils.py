import json
import os

def read_population(hdfs_path, island_id):
    """Lee la poblacion desde HDFS """
    file_path = os.path.join(hdfs_path, f'population_island_{island_id}.json')
    if not os.path.exists(file_path):
        return None
    
    with open(file_path, 'r') as f:
        return json.load(f)

def write_population(hdfs_path, island_id, population):
    """Escribe la poblacion a HDFS"""
    os.makedirs(hdfs_path, exist_ok=True)
    file_path = os.path.join(hdfs_path, f'population_island_{island_id}.json')
    with open(file_path, 'w') as f:
        json.dump(population, f)

def write_elite(island_id, elite, hdfs_path='.'):
    """Guarda los elites de una isla en HDFS"""
    file_path = os.path.join(hdfs_path, f'elite_island_{island_id}.json')
    with open(file_path, 'w') as f:
        json.dump(elite, f)