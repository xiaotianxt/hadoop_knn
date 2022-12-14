import os
import random
import time

times = []

for k in [25]:
    start = time.time()
    for i in range(5):
        x, y = 115.5 + random.random(), 39.5 + random.random()

        os.system(
            f"python mr_query_nn.py -r local --k {k} --location '{x} {y}' output/index.txt > {i}.txt")

    end = time.time()

    times.append((end - start) / 5)

print(*list(zip([3, 5, 10, 15, 25], times)), sep="\n")
