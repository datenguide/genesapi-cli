"""
combine all key json into one
"""


import json
import logging
import sys
import pandas as pd


from genesapi.util import get_files


logger = logging.getLogger(__name__)


def main(args):
    # FIXME make multilangual
    logger.log(logging.INFO, 'Building keys db from `%s` ...' % args.source)
    files = get_files(args.source, lambda x: x.endswith('_de.json'))
    df = pd.DataFrame(json.load(open(f)) for f in files).rename(columns={'code': 'id'})
    df.index = df['id']
    sys.stdout.write(json.dumps(df.T.to_dict()))
