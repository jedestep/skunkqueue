SkunkQueue
==========
_(name change probably pending)_

Task scheduling for humans (who use Python).

### Installing
```sudo python setup.py install```

You will need to install one of the following: (MongoDB)[http://mongodb.org], (Redis)[http://redis.io], or (FoundationDB)[http://foundationdb.com].

### Using SkunkQueue

```python
 # demo.py; this file launches events

from skunkqueue import SkunkQueue
from skunkqueue.persistence.mongodb import default_cfg
from time import sleep

 # Configure our queue. Right now these are the only options to be configured.
 # Backend options are mongodb, redis, and fdb.
 # Persistence modules have a default_cfg field.
 # The first field here is the queue name.
queue = SkunkQueue('demo', **default_cfg)

 # Create an event that will go onto our queue.
 # Workers with this queue/route pair will execute this
 # event when it's fired.
 # Note that events have type skunkqueue.job.Job,
 # not function.
@queue.event(routes=['foo'])
def add(first, second):
    # The return value gets saved by the backend.
    return first + second

 # Now we launch the event.
add.fire(4,5)
sleep(2)
 # As soon as possible, an applicable worker will execute our task.
 # "9" will be printed to the log and stored in the result section
 # of the database.
 # Additionally, the state and result fields of add will be updated.
print 'result', add.result
print 'state', add.state
```

```python
 # demo_worker.py; this file runs a worker to consume events.
 # one day there will be a cli to control this

from skunkqueue.worker import WorkerPool
from skunkqueue.persistence.mongodb import default_cfg

 # The first argument is the queue name to which the pool attaches.
 # The second argument is the list of workers by routing key.
 # If multiple workers have the same routing key,
 # tasks at that key are first come first serve.
 # To make tasks broadcastable, pass queue_type='broadcast'
 # to the SkunkQueue constructor.
with WorkerPool('demo', ['foo'], **default_cfg) as pool:
    pool.listen()
```
