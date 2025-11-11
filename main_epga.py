import json
import subprocess
import time
import csv


def ejecutar_hadoop_streaming(config, poblacion_size):
    # Actualiza el tamaño de la población en el archivo de configuración
    config['population_size'] = poblacion_size

    # Guarda el archivo de configuración modificado
    with open("config.json", "w") as config_file:
        json.dump(config, config_file, indent=4)

    # Comando de Hadoop Streaming
    hadoop_command = [
        "hadoop", "jar", "/home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar",
        "-input", "/Challenge_adge/epga/islands.txt",
        "-output", f"/Challenge_adge/epga/output_xit1083_{poblacion_size}",
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
    print(f"Tiempo de ejecución de Hadoop Streaming para población {poblacion_size}: {elapsed_time:.2f} segundos")

    return elapsed_time  # Retornamos el tiempo de ejecución de EPGA


def guardar_resultados_csv(resultados):
    # Guardar todos los resultados en un archivo CSV
    with open("resultados_epga.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Población", "Tiempo EPGA (s)"])
        for resultado in resultados:
            writer.writerow(resultado)  # Escribir cada fila de resultados


if __name__ == "__main__":
    # Cargar el archivo de configuración
    with open("./epga/config.json", "r") as config_file:
        config = json.load(config_file)
    
    # Definir los tamaños de población a experimentar
    tamaños_poblacion = [500, 1000, 2000, 5000, 10000]

    resultados = []  # Lista para almacenar los resultados

    # Ejecutar EPGA para cada tamaño de población
    for tamaño in tamaños_poblacion:
        # Ejecutar EPGA con Hadoop Streaming
        tiempo_epga = ejecutar_hadoop_streaming(config, tamaño)

        # Mostrar el tiempo de ejecución de EPGA por consola
        print(f"Tiempo de ejecución de EPGA para población {tamaño}: {tiempo_epga:.2f} segundos")

        # Guardar los resultados de este experimento
        resultados.append([tamaño, tiempo_epga])

    # Guardar todos los resultados en un archivo CSV
    guardar_resultados_csv(resultados)
    print("Resultados guardados en 'resultados_epga.csv'")
