from base_subapp import Subapp
from skunkqueue.worker import WorkerPool

import os
import sys

def get_subapp():
    class WorkSubapp(Subapp):
        def parse(self):
            parser = Subapp.parse(self)
            parser.add_argument("-b", "--backend", default="mongodb", choices=["mongodb", "redis", "foundation"], metavar="backend", help="The type of backend to use")
            parser.add_argument("-l", "--logfile", default="__STDOUT", metavar="logfile", help="File to log to, default is stdout")
            parser.add_argument("-p", "--pidfile", metavar="pidfile", help="Pidfile location")
            parser.add_argument("-q", "--queue", help="The name of the queue to bind to")
            parser.add_argument("-r", "--routes", nargs="+", help="A list of workers to create, by route name")
            return parser.parse_args()
        def _run(self):
            # TODO figure out how to import backend
            if self.args.logfile == '__STDOUT':
                logfile = sys.stdout
            else:
                logfile = self.args.logfile
            pid = os.fork()
            if pid == 0:
                with WorkerPool(self.args.queue, self.args.routes, logfile=logfile) as pool:
                    pool.listen()

    return WorkSubapp()
