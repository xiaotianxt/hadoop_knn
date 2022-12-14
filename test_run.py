import os
import random
import time

start = time.time()

for i in range(10):
    x, y = 115.5 + random.random(), 39.5 + random.random()

    os.system(
        f"python mr_query_nn.py -r local --k 10 --location '{x} {y}' output/index.txt > {i}.txt")


end = time.time()

print(end - start)
