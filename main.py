from SGA import SGA
import random
from epga.utils.ga_utils import carga_ciudades

if __name__ == "__main__":

    archivo = "xqf131.csv"

    ciudades = carga_ciudades(archivo)
    
    print("Ejecutando SGA secuencial")
    mejor_ruta, mejor_dist = SGA(
        ciudades, poblacion_size=200, generaciones=1000, elite_size=10, mutation_rate=0.05
    )
    print("\nMejor ruta encontrada por SGA:", mejor_ruta)
    print("Distancia total de la mejor ruta:", mejor_dist)