import random
import concurrent.futures
from epga.utils.ga_utils import fitness

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

def inversion_mutation(individuo, mutation_rate=0.5):
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
    # Usar heap para obtener los mejores individuos
    mejores_individuos = sorted(zip(total_fitness, total), key=lambda x: x[0], reverse=True)[:poblacion_size]
    return [ind for _, ind in mejores_individuos]

# =========================
# Algoritmo Genético Simple (SGA)
# =========================

def evaluar_fitness_individuos(poblacion, ciudades):
    """Evaluación de fitness de los individuos en paralelo."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        fitnessnes = list(executor.map(lambda ind: fitness(ind, ciudades), poblacion))
    return fitnessnes

def SGA(config, ciudades):
    """
    Algoritmo Genético Simple (SGA) utilizando parámetros del archivo config.
    """
    poblacion_size = config['population_size']
    generaciones = config['migration_period']  # Usado como número de generaciones en el paper
    elite_size = config['elite_size']
    mutation_rate = config['mutation_rate']  # Tomando la tasa de mutación desde config.json
    
    # Inicializar la población
    poblacion = inicializacion(poblacion_size, len(ciudades))

    for gen in range(generaciones):
        # Evaluar fitness de los individuos
        fitnessnes = evaluar_fitness_individuos(poblacion, ciudades)
        
        # Elitismo: seleccionar los mejores individuos
        elite = elitismo(poblacion, fitnessnes, elite_size)
        poblacion = [ind for ind in poblacion if ind not in elite]  # Eliminar los mejores de la población para no duplicarlos

        # Generar descendencia a partir de la población
        offspring = []
        while len(offspring) < poblacion_size - elite_size:
            padre1 = torneo(poblacion, fitnessnes)
            padre2 = torneo(poblacion, fitnessnes)
            hijo = escx(padre1, padre2)  # Cruce entre los padres
            offspring.append(hijo)

        # Mutación
        offspring = [inversion_mutation(ind, mutation_rate) for ind in offspring]

        # Evaluar fitness de la descendencia
        fitness_offspring = evaluar_fitness_individuos(offspring, ciudades)

        # Selección de supervivencia: combinar población y descendencia, seleccionar los mejores
        poblacion = supervivencia_seleccion(poblacion, fitnessnes, offspring, fitness_offspring, poblacion_size)
        
        # Añadir los mejores individuos de la élite
        poblacion += elite

    # Evaluar fitness final de la población y seleccionar el mejor individuo
    fitnessnes = evaluar_fitness_individuos(poblacion, ciudades)
    mejor_indice = max(range(len(fitnessnes)), key=lambda i: fitnessnes[i])
    return poblacion[mejor_indice], 1 / fitnessnes[mejor_indice]  # Retornar el mejor individuo y su fitness invertido (distancia)
