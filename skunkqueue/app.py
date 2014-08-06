#!/usr/bin/env python
import skunkqueue.cli.work as work
import skunkqueue.cli.web as web
import sys

if __name__ == '__main__':
    subapp = globals()[sys.argv[1]].get_subapp()
    subapp.run()
