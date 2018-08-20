import json
import os
import re

from datetime import datetime
from multiprocessing import Pool, cpu_count
from slugify import slugify_de
from regenesis.cube import Cube
from regenesis.util import make_key


CPUS = cpu_count()


def get_chunks(iterable, n=CPUS):
    """
    split up an iterable into n chunks
    """
    total = len(iterable)
    if total < n:
        return [[i] for i in iterable]
    chunk_size = int(total / n)
    chunks = []
    for i in range(n):
        if not i+1 == n:
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


def load_cube(fp):
    name = os.path.splitext(os.path.split(fp)[1])[0]
    with open(fp) as f:
        raw = f.read().strip()
    return Cube(name, raw)


def time_to_json(value):
    try:
        return value.isoformat()
    except AttributeError:
        return


def cube_serializer(value):
    value = time_to_json(value)
    if value:
        return value
    try:
        return value.to_dict()
    except AttributeError:
        return


GENESIS_REGIONS = ('dinsg', 'dland', 'regbez', 'kreise', 'gemein')
META_KEYS = GENESIS_REGIONS + ('stag', 'date', 'jahr', 'year', 'id', 'fact_id', 'nuts_level', 'cube')


def slugify_graphql(value, to_lower=True):
    """
    make sure `return` value is graphql key conform,
    meaning no '-' in it
    """
    if not isinstance(value, str):
        return value
    return slugify(value, separator='_', to_lower=to_lower)


def compute_fact_id(fact):
    """because the `fact_id` generated in `regenesis.cube` is not unique"""
    parts = []
    for key, value in fact.items():
        if key.lower() not in META_KEYS or key.lower() in ('id', 'date', 'year', 'cube'):
            if isinstance(value, dict):
                value = value['value']
            parts.append('%s:%s' % (key, value))
    return make_key(sorted(parts))


def serialize_fact(fact, cube_name=None):
    """convert `regensis.cube.Fact` to json-seriable dict"""
    fact = fact.to_dict()
    if cube_name:
        fact['cube'] = cube_name
    for nuts, key in enumerate(GENESIS_REGIONS):
        if fact.get(key.upper()):
            fact['id'] = fact.get(key.upper())
            fact['nuts_level'] = nuts if nuts < 4 else None
            break
    if 'STAG' in fact:
        date = datetime.strptime(fact['STAG']['value'], '%d.%m.%Y').date()
        fact['date'] = date.isoformat()
        fact['year'] = str(date.year)
    if 'JAHR' in fact:
        fact['year'] = fact['JAHR']['value']
    fact = {k.upper() if k.lower() not in META_KEYS else k.lower():
            slugify_graphql(v, False) if k not in META_KEYS else v
            for k, v in fact.items()}
    return json.loads(json.dumps(fact, default=time_to_json))


def clean_description(raw):
    return re.sub('.(\\n).', lambda x: x.group(0).replace('\n', ' '), re.sub('<.*?>', '', raw or '')).strip()


alnum_pattern = re.compile('[\W_]+', re.UNICODE)


def remove_punctuation(value):
    return alnum_pattern.sub(' ', value or '')


def get_fulltext_parts(fact, schema, names):
    schemas = {k: schema[k] for k in fact.keys() if k in schema}
    args = [(k, v) for k, v in fact.items() if k not in tuple(schema.keys()) + META_KEYS]
    parts = [names.get(fact['id']), fact.get('year'), fact['id']]
    for part in parts:
        if part:
            yield part
    for k, info in schemas.items():
        name = remove_punctuation(info.get('name'))
        if name:
            for n in name.split():
                yield n
        source = remove_punctuation(info.get('source', {}).get('title_de'))
        if source:
            for s in source.split():
                yield s
        for arg, value in args:
            arg_info = info.get('args', {}).get(arg)
            if arg_info:
                arg_name = remove_punctuation(arg_info.get('name'))
                if arg_name:
                    for a in arg_name.split():
                        yield a
                value = [v.get('name') for v in arg_info.get('values', []) if v['value'] == value]
                if len(value) and value[0]:
                    for v in remove_punctuation(value[0]).split():
                        yield v
