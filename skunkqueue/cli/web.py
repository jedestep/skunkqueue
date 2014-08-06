from base_subapp import Subapp
from skunkqueue.web import run

import os

def get_subapp():
    class WebSubapp(Subapp):
        def parse(self):
            parser = Subapp.parse(self)
            parser.add_argument("-b", "--backend", default="mongodb", choices=['mongodb', 'redis', 'foundation'], metavar='backend', help="The type of backend to use")
            parser.add_argument("-d", "--dbname", default="skunkqueue", help="The dbname to use")
            parser.add_argument("-c", "--conn", default="localhost:27017", help="The url of the backend. Format is host:port")
            return parser.parse_args()
        def _run(self):
            run(self.args.backend,
                    self.args.conn,
                    self.args.dbname)

    return WebSubapp()
