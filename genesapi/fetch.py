"""
download raw cubes, see `genesapi.storage` for details
"""


import logging

from genesapi.exceptions import StorageDoesNotExist
from genesapi.storage import Storage


logger = logging.getLogger(__name__)


def main(args):
    try:
        storage = Storage(args.storage)
    except StorageDoesNotExist:
        if args.new:
            storage = Storage.create(args.storage)
        else:
            raise StorageDoesNotExist('Storage does not exist at `%s`. If you want to create it, use the --new flag.')

    logger.log(logging.INFO, 'Starting download / update for Storage `%s` ...' % args.storage)
    storage.download(prefix=args.prefix)
    logger.log(logging.INFO, 'Finished download / update for Storage `%s`' % args.storage)
