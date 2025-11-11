"""
#!/home/hadoop/cluster_env/bin/python3

import sys, json
from mapper_utils import mapper


if __name__ == "__main__":
    config = json.load(open("config.json"))
    for line in sys.stdin:
        island_id = int(line.strip())
        mapper(island_id, "/", config)
"""

#!/home/hadoop/cluster_env/bin/python3
import sys, json, time
from utils.mapper_utils import mapper, _metric

if __name__ == "__main__":
    job_t0 = time.perf_counter()
    config = json.load(open("config.json"))
    for line in sys.stdin:
        island_id = int(line.strip())
        mapper(island_id, "/", config)
    _metric("mapper_driver_total_s", time.perf_counter() - job_t0)