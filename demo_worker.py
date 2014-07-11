from skunkqueue.worker import WorkerPool
from time import sleep

pool = WorkerPool('demo', ['foo','foo','foo'])

try:
    while True:
        sleep(0.5)
except KeyboardInterrupt:
    print "^C is caught, exiting"
    pool.shutdown()
