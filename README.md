SkunkQueue
==========
_(name change probably pending)_

Task scheduling for humans (who use Python).

### Installing
```sudo python setup.py install```

You will need to install one of the following: [MongoDB](http://mongodb.org), [Redis](http://redis.io), or [FoundationDB](http://foundationdb.com).
MongoDB is the most well-supported at the moment. Redis is mostly functional and Foundation is experimental. In the future they will all be equally well supported.

### Launching jobs

```python
 # demo.py; this file launches events

from skunkqueue import SkunkQueue
from skunkqueue.persistence.mongodb import default_cfg
from time import sleep
from datetime import timedelta

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
add.fire_at(timedelta(seconds=60),4,5)
sleep(2)
 # As soon as possible, an applicable worker will execute our task.
 # "9" will be printed to the log and stored in the result section
 # of the database.
 # Additionally, the state and result fields of add will be updated.
print 'result', add.result
print 'state', add.state
```

### Executing jobs

Launching workers happens from the command line.

```bash
$ skunq work -q queue_name -r route1 route2 route3
```

The ```-q``` argument specifies the name of the queue to listen from. The ```-r``` argument gives a list of routes. One worker is spun up per route. Multiple workers can exist per route; when a job is sent to that route, that job will be received on a first-come-first-serve basis.

#### Managing Workers

Each worker started through one call to ```skunq``` is a part of a single ```WorkerPool```. The pool serves as a parent process and monitor. Killing its PID will kill _all_ workers started by that pool. The ```WorkerPool``` responds to signals with one of two strategies: _gentle_ or _rough_. Responses to different signals are as follows:
| Signal   | Strategy  | 
|----------|-----------|
| SIGHUP   | gentle    |
| SIGABRT  | gentle    |
| SIGQUIT  | gentle    |
| SIGTERM  | gentle    |
| SIGINT   | rough     |
| SIGKILL  | unhandled |

The gentle strategy will wait for the current job to finish, then unregister and close the worker. The rough strategy will force the current job to raise an exception; no output for the job will be logged. The worker will then be unregistered and closed. ```SIGKILL``` is unhandled and will instantly kill the pool and all workers with no cleanup. __Starting workers after a ```SIGKILL``` currently has undefined behavior. You must manually clear the workers from the database to resume correct behavior.__
