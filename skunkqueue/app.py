#!/usr/bin/env python
import argparse

import skunkqueue.cli.work as work

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("subapp", help="<work|clear|kill>")
    parser.add_argument("rest", nargs="+")
    args = parser.parse_args()
    subapp = globals()[args.subapp].get_subapp()
    subapp.run()
