import random
import math
import subprocess
#import pandas as pd
from io import StringIO

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
    total = distancia_total(ruta, puntos) 
    return 1 / total if total != 0 else 0

def inicializacion(poblacion_size, num_ciudades):
    """Genera una población inicial de rutas aleatorias."""
    poblacion = []
    for _ in range(poblacion_size):
        individuo = list(range(num_ciudades))
        random.shuffle(individuo)
        poblacion.append(individuo)
    return poblacion

def torneo(poblacion, fitnessnes, k=3):
    """Selecciona un individuo usando torneo, adaptable a poblaciones pequeñas."""
    if not poblacion or not fitnessnes:
        return []  # Evita error si no hay individuos

    k = min(k, len(poblacion))  # ajusta k al tamaño disponible
    participantes = random.sample(list(zip(poblacion, fitnessnes)), k)
    participantes.sort(key=lambda x: x[1], reverse=True)
    return participantes[0][0][:]  # copia del mejor

def escx(p1, p2):
    """Edge Sequential Crossover (ESCX) robusto."""
    if not p1 or not p2:  # protección contra secuencias vacías
        return []

    size = len(p1)
    edge_table = {i: set() for i in range(size)}

    for p in [p1, p2]:
        for i in range(size):
            a, b = p[i], p[(i + 1) % size]
            edge_table[a].add(b)
            edge_table[b].add(a)

    # Si no hay genes válidos
    if not any(edge_table.values()):
        return random.sample(p1, len(p1))  # devuelve permutación aleatoria

    try:
        current = random.choice(p1)
    except IndexError:
        return random.sample(range(size), size)

    child = []
    while len(child) < size and current is not None:
        child.append(current)
        for edges in edge_table.values():
            edges.discard(current)

        if edge_table[current]:
            next_city = min(edge_table[current], key=lambda x: len(edge_table[x]))
        else:
            remaining = [c for c in p1 if c not in child]
            next_city = random.choice(remaining) if remaining else None

        current = next_city

    # Rellenar si quedó incompleto
    if len(child) < size:
        remaining = [c for c in p1 if c not in child]
        child.extend(remaining)

    return child

def inversion_mutation(individuo, mutation_rate=0.1):
    """Aplica mutación por inversión a un individuo."""
    if not individuo or len(individuo) < 2:
        return individuo  # nada que mutar
        
    if random.random() < mutation_rate:
        i, j = sorted(random.sample(range(len(individuo)), 2))
        individuo[i:j] = reversed(individuo[i:j])
        
    return individuo

def elitismo(poblacion, fitnessnes, elite_size):
    """Selecciona los mejores individuos para la siguiente generación."""
    elite_idx = sorted(range(len(fitnessnes)), key=lambda i: fitnessnes[i], reverse=True)[:elite_size]
    return [poblacion[i][:] for i in elite_idx]

def supervivencia_seleccion(poblacion, fitnessnes, offspring, fitness_offspring, poblacion_size):
    """Combina la población actual con la descendencia y selecciona los mejores."""
    total = poblacion + offspring
    total_fitness = fitnessnes + fitness_offspring
    orden = sorted(range(len(total)), key=lambda i: total_fitness[i], reverse=True)
    return [total[i] for i in orden[:poblacion_size]]

"""
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
"""