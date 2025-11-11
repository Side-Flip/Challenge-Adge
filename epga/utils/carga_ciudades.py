import pandas as pd
import subprocess
from io import StringIO  # Asegúrate de importar StringIO para leer el contenido del CSV

def carga_ciudades(archivo):
    # Verifica si el archivo tiene una ruta completa
    if not archivo.startswith('challenge_adge/datasets/'):
        archivo = f"challenge_adge/datasets/{archivo}"

    resultado = subprocess.run(
        ["hdfs", "dfs", "-cat", archivo],  # Usa la ruta construida correctamente
        capture_output=True,
        text=True,
        check=True
    )
    
    contenido = resultado.stdout

    # Usar StringIO para leer el contenido del CSV
    df = pd.read_csv(StringIO(contenido))
    
    # Asumimos que las columnas son 'x' y 'y', ajusta según tu archivo CSV
    ciudades = list(zip(df['x'], df['y']))
    
    return ciudades
