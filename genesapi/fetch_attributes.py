"""
download data about attributes
"""


import logging
import json
import os
import string
import yaml
from zeep import Client


logger = logging.getLogger(__name__)


def _get_attribute_keys(client, username, password):
    for a in string.ascii_uppercase:
        logger.log(logging.INFO, 'Retrieving keys starting with `%s` ...' % a)
        with client.settings(strict=False):
            res = client.service.MerkmalsKatalog(
                kennung=username,
                passwort=password,
                filter='%s*' % a,
                kriterium='Code',
                bereich='Alle',
                typ='Alle',
                sprache='de',
                listenLaenge=500
            )

        if len(res.merkmalsKatalogEintraege) == 500:
            logger.log(logging.ERROR, 'There are more than 500 attributes starting with `%s` !' % a)
        else:
            for attribute in res.merkmalsKatalogEintraege:
                code = attribute.find('code').text
                logger.log(logging.INFO, 'found code `%s`' % code)
                yield code


def _get_attribute(client, username, password, code):
    # FIXME make multilangual
    # for lang in ('de', 'en'):
    #     sys.stdout.write('Retrieving information for code "%s" in language "%s" ...\n' % (code, lang))
    try:
        with client.settings(strict=False):
            res = client.service.MerkmalInformation(
                kennung=username,
                passwort=password,
                name=code,
                bereich='alle',
                sprache='de'
            )

        data = {
            'code': code,
            'lang': 'de',
            'description': res.information,
            'name': res.inhalt,
            'type': res.objektTyp
        }
        return data

    except Exception as e:
        logger.log(logging.ERROR, 'Could not download `%s`: %s' (code, str(e)))


def main(args):
    with open(args.catalog) as f:
        catalog = yaml.load(f)

    if not os.path.isdir(args.output):
        raise FileNotFoundError('`%s is not a valid output`' % args.output)

    ResearchClient = Client(wsdl=catalog['index_url'])
    ExportClient = Client(wsdl=catalog['export_url'])
    logger.log(logging.INFO, 'Obtaining attributes from `%s` ...' % catalog['index_url'])

    for code in _get_attribute_keys(ResearchClient, catalog['username'], catalog['password']):
        fp = os.path.join(args.output, '%s.json' % code.replace('-', '_'))
        exists = os.path.isfile(fp)
        if exists and not args.replace:
            logger.log(logging.INFO, 'Key `%s` already exists, skipping ...' % code)
        else:
            if exists and args.replace:
                logger.log(logging.INFO, 'Key `%s` already exists, replacing ...' % code)
            logger.log(logging.INFO, 'Downloading `%s` from `%s` ...' % (code, catalog['export_url']))
            key = _get_attribute(ExportClient, catalog['username'], catalog['password'], code)
            if key:
                with open(fp, 'w') as f:
                    json.dump(key, f)
                logger.log(logging.INFO, 'Saved `%s` to `%s`' % (code, fp))
