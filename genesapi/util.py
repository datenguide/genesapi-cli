import copy
import json
import os
import re
import sys

import dateutil.parser

from datetime import datetime
from multiprocessing import Pool, cpu_count
from slugify import Slugify, GERMAN
from time import strptime
from regenesis.util import make_key


CPUS = cpu_count()


slugify_de = Slugify(pretranslate=GERMAN)


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
        if not i + 1 == n:
            chunks.append(iterable[i * chunk_size:(i + 1) * chunk_size])
        else:
            chunks.append(iterable[i * chunk_size:])
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
        _args = ([a] * CPUS for a in args)
        with Pool(processes=CPUS) as P:
            res = P.starmap(func, zip(chunks, *_args))
    else:
        with Pool(processes=CPUS) as P:
            res = P.map(func, chunks)

    return (i for r in res for i in r)


def slugify(value, to_lower=True, separator='-'):
    return slugify_de(value, to_lower=to_lower, separator=separator)


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
META_KEYS = GENESIS_REGIONS + ('stag', 'date', 'jahr', 'year', 'region_id', 'fact_id',
                               'nuts', 'lau', 'cube', 'statistic', 'region_level', 'measure', 'value')
EXCLUDE_KEYS = GENESIS_REGIONS + ('stag', 'jahr')
EXCLUDE_FACT_ID_KEYS = set(META_KEYS) - set(('region_id', 'date', 'year'))
REGION_LEVEL_NAMES = (  # FIXME internationalization
    ('Deutschland', 'Deutschland'),
    ('Bundesland', 'Bundesländer'),
    ('Regierungsbezirk oder statistische Region', 'Regierungsbezirke und statistische Regionen'),
    ('Kreis oder kreisfreie Stadt', 'Kreise und kreisfreie Städte'),
    ('Gemeinde', 'Gemeinden')
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
    """
    create an id that describes the unique combination of dimensions for a fact
    needed for elasticsearch doc_id and for de-duplication
    """
    # FIXME make sure this is really working as expected  xD

    parts = []
    for key, value in fact.items():
        if key.lower() not in EXCLUDE_FACT_ID_KEYS:
            if isinstance(value, dict):
                value = ''  # the actual value is not an indicator for uniqueness
            parts.append('%s:%s' % (key, value))
    return make_key(sorted(parts))


def get_fact_path(fact):
    # FIXME implementation
    path = {k: {} for k, v in fact.items() if k.isupper() and isinstance(v, dict)}
    for k, v in fact.items():
        if k.isupper() and not isinstance(v, dict):
            for val in path.values():
                val[k] = v
    return path


def get_fact_path_str(fact):
    """
    return a string like `BEVZ20(GES:GESM,ALTX20:ALT075UM)` to describe the
    selection of dimensions (without region & time) for this fact
    [sort args alphabetically]
    """
    # FIXME implementation
    attributes = [k for k, v in fact.items() if k.isupper() and isinstance(v, dict)]
    dimensions = []
    for k, v in fact.items():
        if k.isupper() and not isinstance(v, dict):
            dimensions += [':'.join((k, v))]
    return '%s%s' % (','.join(attributes), ('(%s)' % ','.join(sorted(dimensions)) if dimensions else ''))


def serialize_fact(fact, cube_name=None, flat=False):
    """convert `regensis.cube.Fact` to json-seriable dict"""
    if cube_name:
        fact['cube'] = cube_name
        fact['statistic'] = cube_name[:5]
    for level, key in enumerate(GENESIS_REGIONS):
        if fact.get(key.upper()):
            fact['region_id'] = fact.get(key.upper())
            fact['region_level'] = level
            if level < 4:
                fact['nuts'] = level
            else:
                fact['lau'] = 2
            break
    if 'STAG' in fact:
        date = datetime.strptime(fact['STAG']['value'], '%d.%m.%Y').date()
        fact['date'] = date.isoformat()
        fact['year'] = str(date.year)
        del fact['STAG']
    if 'JAHR' in fact:
        fact['year'] = fact['JAHR']['value']
    # for easier time based analysis:
    if 'date' not in fact and 'year' in fact:
        fact['date'] = datetime(int(fact['year']), 12, 31).date()

    fact = {k.upper() if k.lower() not in META_KEYS else k.lower():
            slugify_graphql(v, False) if k not in META_KEYS else v
            for k, v in fact.items() if k.lower() not in EXCLUDE_KEYS}

    fact['fact_id'] = compute_fact_id(fact)
    fact['path'] = get_fact_path(fact)

    if flat:
        for k, v in fact.items():
            if isinstance(v, dict) and 'value' in v:
                fact[k] = v['value']

    return json.loads(json.dumps(fact, default=time_to_json))


def get_value_from_file(fp, default=None, transform=lambda x: x):
    if os.path.exists(fp):
        with open(fp) as f:
            return transform(f.read().strip())
    return default


def to_date(value, force_ws=False):
    if not force_ws:
        try:
            return dateutil.parser.parse(value)
        except ValueError:
            pass
    # date format in webservice:
    # 07.08.2019 08:40:20h (but the "h" at the end is optional -.- )
    return datetime(*strptime(value.rstrip('h'), '%d.%m.%Y %H:%M:%S')[:6])


iso_regex = re.compile(
    '^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?(Z)?$')  # noqa


def is_isoformat(value):
    return bool(iso_regex.match(value))


def get_fulltext_data(data, cube):
    dimensions = {d: {'name': cube.schema.dimensions[d]['title_de'],
                      'value': cube.schema.dimensions[d]['value_names'][data[d]]}
                  for d in set(cube.schema.dimensions) & set(data)}
    level_name, level_name_plural = REGION_LEVEL_NAMES[data['region_level']]
    return {
        'region_name': cube.schema.regions[data['region_id']],
        'region_level_name': level_name,
        'region_level_name_plural': level_name_plural,
        'statistic_name': cube.schema.statistic['title_de'],
        'measure_name': cube.schema.measures[data['measure']]['title_de'],
        'dimension_names': ', '.join('%s: %s' % (d['name'], d['value']) for d in dimensions.values()),
        'dimensions': dimensions
    }


def unpack_fact(fact, schema):
    """
    if a fact from `regensis.cube.Fact` has more than one root key (`Merkmal`)
    split this fact into as many facts as the original has root keys
    """
    if not isinstance(fact, dict):
        fact = fact.to_dict()
    root_keys = set(schema.measures) & set(fact.keys())
    for key in root_keys:
        new_fact = copy.deepcopy(fact)
        new_fact['measure'] = key
        new_fact['value'] = new_fact[key]['value']
        for obsolete_key in root_keys - set([key]):
            del new_fact[obsolete_key]
        yield new_fact


# https://docs.djangoproject.com/en/2.2/ref/utils/#module-django.utils.functional
class cached_property:
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.

    A cached property can be made out of an existing method:
    (e.g. ``url = cached_property(get_absolute_url)``).
    On Python < 3.6, the optional ``name`` argument must be provided, e.g.
    ``url = cached_property(get_absolute_url, name='url')``.
    """
    name = None

    @staticmethod
    def func(instance):
        raise TypeError(
            'Cannot use cached_property instance without calling '
            '__set_name__() on it.'
        )

    @staticmethod
    def _is_mangled(name):
        return name.startswith('__') and not name.endswith('__')

    def __init__(self, func, name=None):
        if sys.version_info >= (3, 6):
            self.real_func = func
        else:
            func_name = func.__name__
            name = name or func_name
            if not (isinstance(name, str) and name.isidentifier()):
                raise ValueError(
                    "%r can't be used as the name of a cached_property." % name,
                )
            if self._is_mangled(name):
                raise ValueError(
                    'cached_property does not work with mangled methods on '
                    'Python < 3.6 without the appropriate `name` argument. See '
                    'https://docs.djangoproject.com/en/2.2/ref/utils/'
                    '#cached-property-mangled-name',
                )
            self.name = name
            self.func = func
        self.__doc__ = getattr(func, '__doc__')

    def __set_name__(self, owner, name):
        if self.name is None:
            self.name = name
            self.func = self.real_func
        elif name != self.name:
            raise TypeError(
                "Cannot assign the same cached_property to two different names "
                "(%r and %r)." % (self.name, name)
            )

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res
