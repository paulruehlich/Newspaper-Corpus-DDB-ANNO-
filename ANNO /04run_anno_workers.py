#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from multiprocessing import Process
from anno_worker_module import run_worker


def run_all_workers():
    processes = []
    for i in range(1, 13):
        p = Process(target=run_worker, args=(i, 12))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()


if __name__ == "__main__":
    run_all_workers()

