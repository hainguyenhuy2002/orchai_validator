from omegaconf import OmegaConf
import psycopg2
import numpy as np
import os
from collections import Counter


def psql_connect(host, port, database, user, password, **kwargs):
    return psycopg2.connect(
        host=host, 
        port=port,
        database=database, 
        user=user, 
        password=password,
    )

    
config = OmegaConf.load("config/etl.yaml")
cur = psql_connect(**config.dest).cursor()

cur.execute(f"select distinct block_height from {config.dest.table} order by block_height;")
bh = cur.fetchall()
bh = np.array([b[0] for b in bh])
bh.sort()

print(bh.shape)
try:
    print(bh[0], bh[-1])
except:
    pass

diff = np.diff(bh)

c = Counter(diff)
print(c, file=open("validate_etl.txt", 'a'))
with open("validate_etl.txt", 'r') as f:
    lines = f.readlines()
    
    if len(lines) > 0:
        s = min(5, len(lines))
        for l in lines[-s:]:
            print(l, end="")

combine_win_size = config.hp.etl.combine_win_size
for i, d in enumerate(diff):
    if d != combine_win_size:
        print(bh[i], bh[i+1], "|", d)

os.system("pause")