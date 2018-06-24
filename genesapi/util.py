import os

from slugify import slugify_de
from multiprocessing import Pool, cpu_count


CPUS = cpu_count()


def get_chunks(iterable, n=CPUS):
    """
    split up an iterable into n chunks
    """
    total = len(iterable)
    chunk_size = int(total / n)
    chunks = []
    if total < n:
        return [iterable]
    for i in range(n):
        if not i-1 == n:
            chunks.append(iterable[i*chunk_size:(i+1)*chunk_size])
        else:
            chunks.append(iterable[i*chunk_size:])
    return chunks


def parallelize(func, iterable, *args):
    """
    parallelize `func` applied to n chunks of `iterable`
    with optional `args`

    return: flattened generator of `func` returns
    """
    try:
        len(iterable)
    except TypeError:
        iterable = tuple(iterable)

    chunks = get_chunks(iterable, CPUS)

    if args:
        _args = ([a]*CPUS for a in args)
        with Pool(processes=CPUS) as P:
            res = P.starmap(func, zip(chunks, *_args))
    else:
        with Pool(processes=CPUS) as P:
            res = P.map(func, chunks)

    return (i for r in res for i in r)


def get_files(directory, condition=lambda x: True):
    """
    return list of files in given `directory`
    that match `condition` (default: all) incl. subdirectories
    """
    return [os.path.join(d, f) for d, _, fnames in os.walk(directory)
            for f in fnames if condition(f)]


def slugify(value, to_lower=True, separator='-'):
    return slugify_de(value, to_lower=to_lower, separator=separator)


def cast_value(value):
    try:
        if float(value) == int(value):
            return int(value)
        return float(value)
    except ValueError:
        try:
            return eval(str(value))
        except (NameError, SyntaxError):
            return value
