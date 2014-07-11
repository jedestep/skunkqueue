from skunkqueue.worker import WorkerPool
from time import sleep

with WorkerPool('demo', ['foo']) as pool:
    try:
        while True:
            sleep(0.5)
    except KeyboardInterrupt:
        print "^C is caught, exiting"
