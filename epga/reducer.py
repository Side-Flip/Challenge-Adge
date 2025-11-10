#!/home/hadoop/cluster_env/bin/python3
import sys, json
from collections import defaultdict
from reducer_utils import reducer

if __name__ == "__main__":
    try:
        config = json.load(open("config.json"))
        all_individuals = defaultdict(list)

        for line_num, line in enumerate(sys.stdin, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) != 2:
                print(f"[WARN] Línea mal formada {line_num}: {line}", file=sys.stderr)
                continue

            island_id, individual_str = parts

            try:
                individual = json.loads(individual_str)
            except json.JSONDecodeError as e:
                print(f"[ERROR] JSON inválido {line_num}: {e} | {individual_str[:50]}", file=sys.stderr)
                continue

            all_individuals[island_id].append(individual)

        # Procesar cada isla
        for island_id in sorted(all_individuals.keys()):
            individuals = all_individuals[island_id]
            print(f"[INFO] Reducer: isla {island_id} → {len(individuals)} individuos", file=sys.stderr)
            reducer(island_id, individuals, config, hdfs_path='.')

    except Exception as e:
        print(f"[FATAL Reducer] {type(e).__name__}: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)