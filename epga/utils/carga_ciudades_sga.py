import subprocess
import pandas as pd
from io import StringIO

def carga_ciudades(archivo):
    resultado = subprocess.run(
        ["hdfs", "dfs", "-cat", f"challenge_adge/datasets/{archivo}"],
        capture_output=True,
        text=True,
        check=True
    )
    contenido = resultado.stdout

    df = pd.read_csv(StringIO(contenido))
    
    ciudades = list(zip(df['x'], df['y']))
    
    return ciudades