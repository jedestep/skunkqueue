from base_subapp import Subapp
from skunkqueue.worker import WorkerPool

import os

def get_subapp():
    class WorkSubapp(Subapp):
        def parse(self):
            parser = Subapp.parse(self)
            parser.add_argument("queue", help="The name of the queue to bind to")
            parser.add_argument("routes", nargs="+", help="A list of workers to create, by route name")
            parser.add_argument("-b", "--backend", default="mongodb", choices=["mongodb", "redis", "foundation"], metavar="backend", help="The type of queue to bind to")
            return parser.parse_args()
        def _run(self):
            # TODO figure out how to import backend
            pid = os.fork()
            if pid == 0:
                with WorkerPool(self.args.queue, self.args.routes) as pool:
                    pool.listen()

    return WorkSubapp()
