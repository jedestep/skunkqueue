from skunkqueue import skunkqueue

queue = skunkqueue.SkunkQueue('demo', queue_type='broadcast')

@queue.event(routes=['foo'])
def add(first, second):
    print first + second

add.fire(4, 5)
