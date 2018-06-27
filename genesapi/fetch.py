"""
download raw cubes, based on https://github.com/pudo/regenesis
"""


import logging
import os
import yaml

from regenesis.retrieve import fetch_index, fetch_cube


logger = logging.getLogger(__name__)


def main(args):
    with open(args.catalog) as f:
        catalog = yaml.load(f)

    if not os.path.isdir(args.output):
        raise FileNotFoundError('`%s is not a valid output`' % args.output)

    logger.log(logging.INFO, 'Obtaining index from `%s` ...' % catalog['index_url'])
    for name in fetch_index(catalog):
        fp = os.path.join(args.output, '%s.csv' % name)
        if os.path.isfile(fp):
            logger.log(logging.INFO, 'Cube `%s` already exists, skipping ...' % name)
        else:
            logger.log(logging.INFO, 'Downloading `%s` from `%s` ...' % (name, catalog['export_url']))
            cube = fetch_cube(catalog, name)
            if cube:
                with open(fp, 'w') as f:
                    f.write(cube)
                logger.log(logging.INFO, 'Saved `%s` to `%s`' % (name, fp))
            else:
                logger.log(logging.ERROR, 'No cube data for `%s`' % name)
