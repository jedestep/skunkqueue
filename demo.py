from skunkqueue import skunkqueue

queue = skunkqueue.SkunkQueue('demo')

@queue.event(routes=['foo'])
def add(first, second):
    return first + second

add.fire(4, 5)