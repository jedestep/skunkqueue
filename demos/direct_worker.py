from skunkqueue.worker import WorkerPool

with WorkerPool('demo', ['foo']) as pool:
    pool.listen()
