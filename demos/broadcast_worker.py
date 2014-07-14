from skunkqueue.worker import WorkerPool

with WorkerPool('demo', ['foo','foo','foo']) as pool:
    pool.listen()
