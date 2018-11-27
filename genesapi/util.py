import copy
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
META_KEYS = GENESIS_REGIONS + (
    'stag', 'date', 'jahr', 'year', 'id', 'fact_id', 'nuts_level', 'cube', 'fact_key', 'fact_value'
)


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


def unpack_fact(fact, schema):
    """
    if a fact from `regensis.cube.Fact` has more than one root key (`Merkmal`)
    split this fact into as many facts as the original has root keys
    """
    if not isinstance(fact, dict):
        fact = fact.to_dict()
    root_keys = set(schema.keys()) & set(fact.keys())
    for key in root_keys:
        new_fact = copy.deepcopy(fact)
        new_fact['fact_key'] = key
        new_fact['fact_value'] = new_fact[key]['value']
        for obsolete_key in root_keys - set([key]):
            del new_fact[obsolete_key]
        yield new_fact


def serialize_fact(fact, cube_name=None):
    """convert `regensis.cube.Fact` to json-seriable dict"""
    if cube_name:
        fact['cube'] = cube_name
    for nuts, key in enumerate(GENESIS_REGIONS):
        key = key.upper()
        if fact.get(key):
            fact['id'] = fact.get(key)
            fact['nuts_level'] = nuts if nuts < 4 else None
            del fact[key]
            break
    if 'STAG' in fact:
        date = datetime.strptime(fact['STAG']['value'], '%d.%m.%Y').date()
        fact['date'] = date.isoformat()
        fact['year'] = str(date.year)
        del fact['STAG']
    if 'JAHR' in fact:
        fact['year'] = fact['JAHR']['value']
        del fact['JAHR']
    fact = {k.upper() if k.lower() not in META_KEYS else k.lower():
            slugify_graphql(v, False) if k not in META_KEYS else v
            for k, v in fact.items()}
    return json.loads(json.dumps(fact, default=time_to_json))


def clean_description(raw):
    return re.sub('.(\\n).', lambda x: x.group(0).replace('\n', ' '), re.sub('<.*?>', '', raw or '')).strip()


alnum_pattern = re.compile('[\W_]+', re.UNICODE)


# def remove_punctuation(value):
#     return alnum_pattern.sub(' ', value or '')


def get_fulltext_parts(fact, schema, names):
    key = schema[fact['fact_key']]
    exclude = tuple(schema.keys()) + META_KEYS
    args = [(k, v) for k, v in fact.items() if k not in exclude]
    parts = [names.get(fact['id']), fact.get('year'), fact['id']]
    for part in parts:
        if part:
            yield part
    name = key.get('name')
    if name:
        yield name
    source = key.get('source', {}).get('title_de')
    if source:
        yield source
    for arg, value in args:
        arg_info = key.get('args', {}).get(arg)
        if arg_info:
            arg_name = arg_info.get('name')
            if arg_name:
                yield arg_name
            value = [v.get('name') for v in arg_info.get('values', []) if v['value'] == value]
            if len(value) and value[0]:
                yield value[0]


def get_fact_context(data, schema, names):
    return {
        'region_name': names.get(data['id']),
        'key_name': schema.get(data['fact_key'], {}).get('name'),
        'source_name': schema.get(data['fact_key'], {}).get('source', {}).get('title_de'),
    }


def get_fulltext_data(data, args):
    parts = list(get_fulltext_parts(data, args.schema, args.names))
    suggestions = [data['id']] + list({p for p in parts if len(p) > 5})
    context = [data['id']] + [c for c in get_fact_context(data, args.schema, args.names).values() if c]
    return {
        'fulltext': ' '.join(parts),
        'fulltext_suggest': suggestions,
        'fulltext_suggest_context': {
            'input': suggestions,
            'contexts': {
                'suggest_context': [c.lower() for c in context]
            }
        }
    }
