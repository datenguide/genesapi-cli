"""
build jekyll frontmatter markdown files for schema keys
"""


import json
import os
import logging
import frontmatter

from genesapi.util import parallelize


logger = logging.getLogger(__name__)


def _build_item(data):
    content = data.pop('description') or data['name']
    return frontmatter.Post(content, **data)


def _process_items(items, output):
    res = []
    for key, data in items:
        matter = _build_item(data)
        fp = os.path.join(output, '%s.md' % key.lower())
        with open(fp, 'w') as f:
            f.write(frontmatter.dumps(matter))
        res.append((key, fp))
    return res


def main(args):
    if not os.path.isdir(args.output):
        logger.log(logging.ERROR, 'output `%s` not valid.' % args.output)
        raise FileNotFoundError(args.output)

    with open(args.schema) as f:
        schema = json.load(f)

    items = parallelize(_process_items, list(schema.items()), args.output)
    for key, fp in items:
        logger.log(logging.INFO, 'Saved `%s` to `%s`' % (key, fp))
