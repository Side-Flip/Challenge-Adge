#Para funciones compartidas que usaran ambos algoritmos

import random
import math

def distancia(p1, p2):
    """Calcula la distancia euclidiana entre dos puntos."""
    return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)


def distancia_total(ruta, puntos):
    """Calcula la distancia total de una ruta dada una lista de puntos."""
    total = 0
    for i in range(len(ruta)):
        punto_actual = puntos[ruta[i]]
        punto_siguiente = puntos[ruta[(i + 1) % len(ruta)]]
        total += distancia(punto_actual, punto_siguiente)
    return total

def fitness(ruta, puntos):
    """Calcula la función de fitness como el inverso de la distancia total."""
    return 1 / distancia_total(ruta, puntos)

