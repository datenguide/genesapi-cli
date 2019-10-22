"""
download raw cubes, see `genesapi.storage` for details
"""


import logging

from genesapi.exceptions import StorageDoesNotExist
from genesapi.storage import Storage


logger = logging.getLogger(__name__)


def main(args):
    try:
        storage = Storage(args.storage, filelogging=args.cronjob)
    except StorageDoesNotExist:
        if args.new:
            storage = Storage.create(args.storage, filelogging=args.cronjob)
        else:
            raise StorageDoesNotExist(
                'Storage does not exist at `%s`. If you want to create it, use the --new flag.' %
                args.storage)

    logger.log(logging.INFO, 'Starting download / update for Storage `%s` ...' % args.storage)
    storage.update(prefix=args.prefix, force=args.force_update)
    logger.log(logging.INFO, 'Finished download / update for Storage `%s`' % args.storage)
