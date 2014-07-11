from skunkqueue import skunkqueue
from time import sleep

queue = skunkqueue.SkunkQueue('demo')

@queue.event(routes=['foo'])
def add(first, second):
    return first + second

add.fire(4, 5)
sleep(2)
print 'result', add.result
print 'state', add.state
