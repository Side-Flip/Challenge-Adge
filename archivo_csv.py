import tsplib95 as tsp
import subprocess
import pandas as pd
import tempfile
import os 

def carga_tsp ():
    
    resultado = subprocess.run(
        ["hdfs", "dfs", "-cat", "challenge_adge/datasets/xqf131.tsp"],
        capture_output=True,
        text=True,
        check=True
    )
    contenido = resultado.stdout

    with tempfile.NamedTemporaryFile(delete=False, suffix=".tsp") as temp_tsp:
        temp_tsp.write(contenido.encode("utf-8"))
        ruta_local_tsp = temp_tsp.name

    xqf131 = tsp.load(ruta_local_tsp)
    nodos131 = xqf131.node_coords

    df131 = pd.DataFrame.from_dict(nodos131, orient='index', columns=['x', 'y'])
    df131.index.name = 'node'

    ruta_csv_local = ruta_local_tsp.replace(".tsp", ".csv")
    df131.to_csv(ruta_csv_local, index=True)

    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", ruta_csv_local, "challenge_adge/datasets/xqf131.csv"],
        check=True
    )

    # Eliminar archivos temporales locales
    os.remove(ruta_local_tsp)
    os.remove(ruta_csv_local)

    print("Archivo xqf131.csv guardado en HDFS correctamente.")
    
    return df131

if __name__ == "__main__":
    print("El archivo se está ejecutando correctamente.")
    carga_tsp()