import math
from utils import fitness

# Operadores geneticos

def inicializacion(poblacion_size, num_ciudades):
    """Genera una población inicial de rutas aleatorias."""
    poblacion = []
    for _ in range(poblacion_size):
        individuo = list(range(num_ciudades))
        random.shuffle(individuo)
        poblacion.append(individuo)
    return poblacion

def torneo(poblacion, fitnessnes, k=3):
    """Selecciona un individuo usando el método de torneo."""
    participantes = random.sample(list(zip(poblacion, fitnessnes)), k)
    participantes.sort(key=lambda x: x[1], reverse=True)
    return participantes[0][0][:]

def escx(p1, p2):
    """Edge Secuential Crossover (ESCX)"""
    size = len(p1)
    edge_table = {i: set() for i in range(size)}
    for p in [p1, p2]:
        for i in range(size):
            a, b = p[i], p[(i + 1) % size]
            edge_table[a].add(b)
            edge_table[b].add(a)
    child = []
    current = random.choice(p1)
    while len(child) < size:
        child.append(current)
        for edges in edge_table.values():
            edges.discard(current)
        if edge_table[current]:
            next_city = min(edge_table[current], key=lambda x: len(edge_table[x]))
        else:
            remaining = [c for c in p1 if c not in child]
            next_city = random.choice(remaining) if remaining else None
        current = next_city
        if current is None:
            break
    return child

def inversion_mutation(individuo, mutation_rate=0.1):
    """Aplica mutación por inversión a un individuo."""
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
    orden = sorted(range(len(total)), key=lambda i: total_fitness[i], reserve=True)
    return [total[i] for i in orden[:poblacion_size]]

# =========================
# Algoritmo Genético Simple
# =========================

def SGA(ciudades, poblacion_size=100, generaciones=500, elite_size=5, mutation_rate=0.1):
    poblacion = inicializacion(poblacion_size, len(ciudades))

    for gen in range(generaciones):
        fitnessnes = [fitness(ind, ciudades) for ind in poblacion]
        elite = elitismo(poblacion, fitnessnes, elite_size)
        poblacion = [ind for ind in poblacion if ind not in elite]

        offspring = []
        while len(offspring) < poblacion_size - elite_size:
            padre1 = torneo(poblacion, fitnessnes)
            padre2 = torneo(poblacion, fitnessnes)
            hijo = escx(padre1, padre2)
            offspring.append(hijo)

        offspring = [inversion_mutation(ind, mutation_rate) for ind in offspring]
        fitness_offspring = [fitness(ind, ciudades) for ind in offspring]

        poblacion = supervivencia_seleccion(poblacion, fitnessnes, offspring, fitness_offspring, poblacion_size)
        poblacion += elite

    fitnessnes = [fitness(ind, ciudades) for ind in poblacion]
    mejor_indice = max(range(len(fitnessnes)), key=lambda i: fitnessnes[i])
    return poblacion[mejor_indice], 1 / fitnessnes[mejor_indice]