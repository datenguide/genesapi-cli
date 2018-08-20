"""
jsonify cubes records
"""


import json
import logging
import os
import sys

from genesapi.util import (
    compute_fact_id,
    get_files,
    load_cube,
    serialize_fact,
    parallelize,
    get_fulltext_parts
)


logger = logging.getLogger(__name__)


def _get_fact(fact, cube_name, args):
    data = serialize_fact(fact, cube_name)
    id_ = compute_fact_id(data)
    data['fact_id'] = id_
    if args.fulltext:
        parts = list(get_fulltext_parts(data, args.schema, args.names))
        data['fulltext'] = ' '.join(parts)
        data['fulltext_suggest'] = list({p for p in parts if len(p) > 5})
    if args.output:
        path = os.path.join(args.output, cube_name)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, '%s.json' % id_), 'w') as f:
            if args.pretty:
                json.dump(data, f, indent=2)
            else:
                json.dump(data, f)
    else:
        if args.pretty:
            return json.dumps(data, indent=2)
        else:
            return json.dumps(data)


def _get_facts(facts, cube_name, args):
    res = []
    for fact in facts:
        serialized_fact = _get_fact(fact, cube_name, args)
        res.append(serialized_fact)
    return res


def _get_json_facts(files, args):
    for i, file in enumerate(files):
        logger.log(logging.INFO, 'Loading cube `%s` (%s of %s) ...' % (file, i+1, len(files)))
        cube = load_cube(file)
        facts = parallelize(_get_facts, cube.facts, cube.name, args)
        for fact in facts:
            yield fact


def main(args):
    if args.output and not os.path.isdir(args.output):
        logger.log(logging.ERROR, 'output `%s` not valid.' % args.output)
        raise FileNotFoundError(args.output)

    if args.fulltext and not args.schema:
        raise Exception('If you want to index fulltext, specify a `schema` json file!')

    files = get_files(args.directory, lambda x: x.endswith('.csv'))

    if args.fulltext:
        with open(args.schema) as f:
            args.schema = json.load(f)
        if args.names:
            with open(args.names) as f:
                args.names = json.load(f)
        else:
            args.names = {}

    facts = _get_json_facts(files, args)
    for fact in facts:
        if not args.output:
            sys.stdout.write(fact + '\n')
