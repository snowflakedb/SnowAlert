import traceback
import sys


def write(*args, stream=sys.stdout):
    if all(isinstance(a, str) for a in args):
        print(*args, sep='\n', file=stream, flush=True)
        traceback.print_exc(file=stream)
    else:
        for a in args:
            if isinstance(a, Exception):
                traceback.print_exception(type(a), a, a.__traceback__, file=stream)
                stream.flush()
            else:
                print(a, file=stream, flush=True)


def info(*args):
    write(*args, stream=sys.stdout)


def error(*args):
    write(*args, stream=sys.stderr)


def fatal(*args):
    error(*args)
    sys.exit(1)
