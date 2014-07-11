from job import Job

class SkunkQueue(object):
    def __init__(self, name):
        self.cfg = {}
        self.handlers = {}

        self.name = name

    def configure(self, conn_url='localhost:27017',
                 dbname='skunkqueue', collname='messages',
                 **kwargs):

        # do configuration
        self.cfg['conn_url'] = conn_url
        self.cfg['dbname'] = dbname
        self.cfg['collname'] = collname

        for k,v in kwargs:
            self.cfg[k] = v

    def add_handler(self, route, handler):
        self.handlers.update({route:handler})

    def event(self, routes=[]):
        def decorator(fn):
            job = Job(self, fn, routes=routes)
            return job
        return decorator

    def __getitem__(self, item):
        return self.cfg[item]
