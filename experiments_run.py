import json
import subprocess

# Cargar la configuración desde el archivo JSON
with open("epga/experiments_config.json") as f:
    config = json.load(f)

# Iterar sobre cada experimento
for experiment in config['experiments']:

    # Guardar la configuración del experimento en config.json
    with open("config.json", "w") as config_file:
        json.dump(experiment, config_file)

    print(f"Ejecutando experimento con tamaño de población {experiment['population_size']}...")

    # Comando de Hadoop
    command = [
        "hadoop", "jar", "/home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar",  # Ruta absoluta al JAR
        "-input", "/Challenge_adge/epga/islands.txt", 
        "-output", f"/Challenge_adge/epga/output_{experiment['population_size']}",
        "-mapper", "python3 mapper.py",
        "-reducer", "python3 reducer.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/mapper.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/reducer.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/config.json",  # Pasando el config.json generado
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/mapper_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/reducer_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/ga_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/hdfs_utils.py",
        "-file", "/home/hadoop/Challenge-Adge/epga/utils/__init__.py",
        "-cmdenv", "PYTHONPATH=."
    ]

    # Ejecutar el comando de Hadoop
    subprocess.run(command)

    # Registrar el tiempo de ejecución
    with open(f"experiment_{experiment['population_size']}_time_log.txt", "a") as log_file:
        log_file.write(f"Experimento con población {experiment['population_size']} ejecutado.\n")
