import json
import os
import subprocess
import sys

def read_cities(hdfs_path):
    """ Leer un archivo de HDFS y convertirlo en una lista de coordenadas (x,y)."""
    try:
        result = subprocess.run(["hdfs", "dfs", "-cat", hdfs_path], capture_output=True, text=True, check=True)
        lines = result.stdout.strip().split("\n")

        puntos = []
        for line in lines:
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) >= 3:
                _, x_str, y_str = parts[:3]
                puntos.append((float(x_str), float(y_str)))
        
        return puntos
    except subprocess.CalledProcessError as e:
        print(f"Error reading from HDFS: {e}", file=sys.stderr)
        sys.exit(1)

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