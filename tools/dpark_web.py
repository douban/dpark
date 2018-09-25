#!/usr/bin/env python


from argparse import ArgumentParser
import sys
import json

from dpark.web import run
from dpark.web.ui import create_app
from dpark.utils import dag


def main():
    parser = ArgumentParser()
    parser.add_argument("-p", "--port",
                        type=int,
                        default=9000)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-l", "--log",
                       help="from loghub dir.  e.g. examples/ui/loghub")
    group.add_argument("-d", "--dag",
                       help="from intermediate dag file. e.g. examples/ui/dag.json")
    parser.add_argument("-g", "--gen",
                        help="gen intermediate dag file from loghub dir, used with -l")

    args = parser.parse_args()

    if args.log:
        context = dag.from_loghub(args.log)
        context = json.dumps(context, indent=4)

        if args.gen:
            with open(args.gen, "w") as f:
                f.write(context)
            return
    else:
        with open(args.dag) as f:
            context = f.read()

    app = create_app(context)
    run(app, args.port, sys.stderr)


if __name__ == "__main__":
    main()
