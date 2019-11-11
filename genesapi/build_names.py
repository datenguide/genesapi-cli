"""
build name mapping for regions

{region_id=> region_name}
"""


import json
import logging
import sys

from genesapi.storage import Storage


logger = logging.getLogger(__name__)


def main(args):
    storage = Storage(args.storage)
    names = {}
    for cube in storage:
        logger.info('Loading `%s` ...' % cube.name)
        names = {**names, **cube.schema.regions}

    sys.stdout.write(json.dumps(names))
