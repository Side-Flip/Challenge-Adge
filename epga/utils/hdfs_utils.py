# hdfs_utils.py
import json
import os
import subprocess
import sys

def read_cities(hdfs_path):
    """Lee un archivo CSV desde HDFS y devuelve lista de (x, y)."""
    try:
        result = subprocess.run(
            ["/home/hadoop/hadoop/bin/hdfs", "dfs", "-cat", hdfs_path],
            capture_output=True, text=True, check=True
        )
        lines = result.stdout.strip().split("\n")
        puntos = []
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            # Ignorar cabecera si existe
            if line.lower().startswith(("x", "city", "id")):
                continue
            parts = line.split(",")
            if len(parts) < 2:
                continue
            try:
                x = float(parts[0].strip())
                y = float(parts[1].strip())
                puntos.append((x, y))
            except ValueError as e:
                print(f"[WARN] Línea {line_num} inválida en {hdfs_path}: {line}", file=sys.stderr)
                continue
        print(f"[INFO] read_cities: {len(puntos)} ciudades leídas desde {hdfs_path}", file=sys.stderr)
        return puntos

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] HDFS error al leer {hdfs_path}: {e.stderr.strip()}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[ERROR] read_cities({hdfs_path}) falló: {e}", file=sys.stderr)
        return []


def read_population(hdfs_path, island_id):
    file_path = os.path.join(hdfs_path, f'population_island_{island_id}.json')
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] read_population({file_path}): {e}", file=sys.stderr)
        return None


def write_population(hdfs_path, island_id, population):
    json_data = json.dumps(population)
    hdfs_output = os.path.join(f"{hdfs_path}/output", f"population_island_{island_id}.json")
    try:
        process = subprocess.Popen(
            ["/home/hadoop/hadoop/bin/hdfs", "dfs", "-put", "-f", "-", hdfs_output],
            stdin=subprocess.PIPE, text=True
        )
        process.communicate(json_data)
        if process.returncode != 0:
            print(f"[ERROR] write_population falló: código {process.returncode}", file=sys.stderr)
    except Exception as e:
        print(f"[ERROR] write_population({hdfs_output}): {e}", file=sys.stderr)


def write_elite(island_id, elite, hdfs_path='.'):
    file_path = os.path.join(hdfs_path, f'elite_island_{island_id}.json')
    try:
        with open(file_path, 'w') as f:
            json.dump(elite, f)
    except Exception as e:
        print(f"[ERROR] write_elite({file_path}): {e}", file=sys.stderr)