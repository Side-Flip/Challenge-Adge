import json
import subprocess
import time
import csv
from SGA import SGA
from epga.utils.ga_utils import carga_ciudades

def ejecutar_sga(config, poblacion_size):
    # Actualiza el tamaño de la población en el archivo de configuración
    config['population_size'] = poblacion_size

    # Guarda el archivo de configuración modificado
    with open("config.json", "w") as config_file:
        json.dump(config, config_file, indent=4)

    # Cargar el archivo de ciudades para SGA
    ciudades = carga_ciudades(config['cities_path'])
    
    # Ejecutar el algoritmo SGA secuencial y medir el tiempo de ejecución
    start_time = time.time()
    print(f"Ejecutando SGA secuencial con población de {poblacion_size}")
    mejor_ruta, mejor_dist = SGA(
        ciudades, poblacion_size=poblacion_size, generaciones=1000, elite_size=10, mutation_rate=0.05
    )
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Guardar los resultados de SGA en un archivo
    return elapsed_time  # Retornamos el tiempo de ejecución de SGA

def ejecutar_hadoop_streaming(config, poblacion_size):
    # Actualiza el tamaño de la población en el archivo de configuración
    config['population_size'] = poblacion_size

    # Guarda el archivo de configuración modificado
    with open("config.json", "w") as config_file:
        json.dump(config, config_file, indent=4)

    # Comando de Hadoop Streaming
    hadoop_command = [
        "hadoop", "jar", "~/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar",
        "-input", "/Challenge_adge/epga/islands.txt",
        "-output", f"/Challenge_adge/epga/output_{poblacion_size}",
        "-mapper", "python3 mapper.py",
        "-reducer", "python3 reducer.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/mapper.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/reducer.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/config.json",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/mapper_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/reducer_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/ga_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/hdfs_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/__init__.py",
        "-cmdenv", "PYTHONPATH=."
    ]

    # Ejecutar el comando de Hadoop
    print(f"Ejecutando Hadoop Streaming para población {poblacion_size}...")
    start_time = time.time()
    
    try:
        subprocess.check_call(hadoop_command)  # Ejecuta el comando de Hadoop
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar Hadoop: {e}")
        return

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución de Hadoop Streaming para población {poblacion_size}: {elapsed_time} segundos")

    return elapsed_time  # Retornamos el tiempo de ejecución de EPGA

def guardar_resultados_csv(resultados):
    # Guardar todos los resultados en un archivo CSV
    with open("resultados_completos.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Población", "Tiempo SGA (s)", "Tiempo EPGA (s)", "Speedup"])
        for resultado in resultados:
            writer.writerow(resultado)  # Escribir cada fila de resultados

if __name__ == "__main__":
    # Cargar el archivo de configuración
    with open("config.json", "r") as config_file:
        config = json.load(config_file)
    
    # Definir los tamaños de población a experimentar
    tamaños_poblacion = [500, 1000, 2000, 5000, 10000]

    resultados = []  # Lista para almacenar los resultados

    # Ejecutar SGA y EPGA para cada tamaño de población
    for tamaño in tamaños_poblacion:
        # Ejecutar SGA
        tiempo_sga = ejecutar_sga(config, tamaño)

        # Ejecutar EPGA con Hadoop Streaming
        tiempo_epga = ejecutar_hadoop_streaming(config, tamaño)

        # Calcular el speedup
        if tiempo_epga > 0:  # Evitar dividir por 0
            speedup = tiempo_sga / tiempo_epga
        else:
            speedup = 0  # En caso de que el tiempo de EPGA sea 0 (lo cual no debería suceder)

        # Guardar los resultados de este experimento
        resultados.append([tamaño, tiempo_sga, tiempo_epga, speedup])

    # Guardar todos los resultados en un archivo CSV
    guardar_resultados_csv(resultados)
    print("Resultados guardados en 'resultados_completos.csv'")
