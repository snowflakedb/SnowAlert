import traceback
import sys


def error(*args):
    if all(isinstance(a, str) for a in args):
        print(*args, sep='\n', file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
    else:
        for a in args:
            if isinstance(a, Exception):
                traceback.print_exception(type(a), a, a.__traceback__)
            else:
                print(a, file=sys.stderr)


def fatal(*args):
    error(*args)
    sys.exit(1)
