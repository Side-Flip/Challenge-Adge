from SGA import SGA
import random

if __name__ == "__main__":
    #Por ahora generar ciudades aleatoriamente pero en el futuro leer desde un archivo
    num_ciudades = 20
    ciudades = [(random.uniform(0, 100), random.uniform(0, 100)) for _ in range(num_ciudades)]

    print("Ejecutando SGA secuencial")
    mejor_ruta, mejor_dist = SGA(
        ciudades, poblacion_size=200, generaciones=1000, elite_size=10, mutation_rate=0.05
    )
    print("\nMejor ruta encontrada por SGA:", mejor_ruta)
    print("Distancia total de la mejor ruta:", mejor_dist)