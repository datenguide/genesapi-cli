"""
transform a source GENESIS csv table into a csv table that
the genesapi data pipeline can consume,
transformation happens based on a yaml specification
for each table

each csv-file needs a metadata file with the same name
(but .yaml-extension instead of .csv) where the info
for `helpers.transform.csv_to_pandas` goes in (see docstring there)

as a convention, filenames should map to the table id from Genesis.
"""


import logging
import json
import os
import sys
import yaml
from copy import deepcopy

from genesapi.helpers.clean import csv_to_pandas
from genesapi.util import get_files


logger = logging.getLogger(__name__)


def _load_yaml(fp):
    logger.log(logging.DEBUG, 'Trying `%s` as yaml spec ...' % fp)
    with open(fp) as f:
        return yaml.load(f.read().strip())


def _get_yaml(fpath, yaml_dir, yaml_fp='', defaults={}):
    """
    look for `.yaml` file with same name as fpath,
    first in same directory, then in given `yaml_dir`

    also load actual yaml (if found) and return as python object
    """
    defaults = deepcopy(defaults)

    if os.path.isfile(yaml_fp):
        defaults.update(_load_yaml(yaml_fp))
        return defaults

    yaml_fp = '%s.yaml' % os.path.splitext(fpath)[0]
    if os.path.isfile(yaml_fp):
        defaults.update(_load_yaml(yaml_fp))
        return defaults
    yaml_fp = os.path.join(yaml_dir, os.path.split(yaml_fp)[1])
    if os.path.isfile(yaml_fp):
        defaults.update(_load_yaml(yaml_fp))
        return defaults
    raise FileNotFoundError('Could not find yaml for `%s`' % fpath)


def _get_dtypes(dtypes, spec={}):
    dtypes = dtypes.map(str).T.to_dict()
    dtypes.update(spec.get('dtype', {}))
    return dtypes


def _get_tocsv_kwargs(df):
    if '_id' in df.columns:
        return {'index': False}
    return {'index_label': '_id'}


def main(args):
    source = args.source
    yaml_fp = args.yaml or ''
    yaml_dir = args.yaml_dir or ''
    target_dir = args.target_dir or args.yaml_dir or source

    with open(args.defaults) as f:
        logger.log(logging.DEBUG, 'Using defaults: `%s` ...' % args.defaults)
        defaults = yaml.load(f.read().strip())

    if os.path.isfile(source):
        spec = _get_yaml(source, yaml_dir, yaml_fp, defaults)
        logger.log(logging.DEBUG, 'Spec: %s' % json.dumps(spec))
        df = csv_to_pandas(source, spec)
        if args.head:
            df = df.head(args.head)
        df.fillna('').to_csv(sys.stdout, **_get_tocsv_kwargs(df))
        if args.dtypes:
            dtypes = _get_dtypes(df.dtypes, spec)
            with open(args.dtypes, 'w') as f:
                json.dump(dtypes, f)
            logger.log(logging.INFO, 'Stored dtypes to `%s` .' % args.dtypes)

    elif os.path.isdir(source):
        logger.log(logging.DEBUG, 'Outputting to target `%s` ...' % target_dir)
        files = get_files(source, lambda x: x.endswith('.csv'))
        dtypes = {}

        for fp in files:
            logger.log(logging.INFO, 'Loading `%s` ...' % fp)
            try:
                spec = _get_yaml(fp, yaml_dir, '', defaults)
                logger.log(logging.DEBUG, 'Spec: %s' % json.dumps(spec))
                df = csv_to_pandas(fp, spec)
                target_fp = os.path.join(target_dir, '%s.cleaned' % os.path.split(fp)[1])
                df.fillna('').to_csv(target_fp, **_get_tocsv_kwargs(df))
                logger.log(logging.INFO, 'Saved to `%s` .' % target_fp)
                if args.dtypes:
                    dtypes.update(_get_dtypes(df.dtypes, spec))
            except FileNotFoundError:
                logger.log(logging.INFO, 'Skipping `%s` because no yaml spec found ...' % fp)

        if args.dtypes:
            with open(args.dtypes, 'w') as f:
                json.dump(dtypes, f)
            logger.log(logging.INFO, 'Stored dtypes to `%s` .' % args.dtypes)

    else:
        logger.log(logging.ERROR, 'source `%s` not valid.' % source)
