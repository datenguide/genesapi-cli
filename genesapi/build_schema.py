"""
build schema out of cubes


schema layout:

- statistic:
  - attribute:
    - dimension:
      - value

{
  "12111": {
    "title_de": "Zensus 2011",
    "title_en": "2011 census",
    "description_de": "...",
    "valid_from": "2011-05-09T00:00:00",
    "periodicity": "EINMALIG",
    "name": "12111",
    "attributes": {
      "BEVZ20": {
        "name": "BEVZ20",
        "title_de": "Bevölkerung",
        "measure_type": "W-MM",
        "atemporal": true,
        "meta_variable": false,
        "valid_from": "1950-01-01T00:00:00",
        "summable": true,
        "title_en": "Bevölkerung",
        "definition_de": "...",
        "values": [],
        "dimensions": {
          "ALTX20": {
            "name": "ALTX20",
            "title_de": "Altersgruppen (unter 3 bis 75 u. m.)",
            "measure_type": "K-SACH-MM",
            "atemporal": false,
            "meta_variable": false,
            "valid_from": "1950-01-01T00:00:00",
            "GLIED_TYP": "DAVON",
            "STD_SORT": "FS",
            "summable": false,
            "title_en": "Altersgruppen (unter 3 bis 75 u. m.)",
            "definition_de": "...",
            "values": [
              {
                "title_de": "unter 3 Jahre",
                "title_en": "unter 3 Jahre",
                "name": "ALT000B03",
                "dimension_name": "ALTX20",
                "value_id": "dd25a6d4cf0a23fd750fb618196b4ad351badbbf",
                "key": "ALT000B03"
              },
            ...

"""


import json
import logging
import sys

from genesapi.storage import Storage, CubeSchema
from genesapi.util import cube_serializer


logger = logging.getLogger(__name__)


def _dumper(value):
    if isinstance(value, set):
        return list(value)
    return cube_serializer(value)


def main(args):
    storage = Storage(args.directory)
    schema = {}
    for cube in storage._cubes:
        logger.info('Loading `%s` ...' % cube.name)
        try:
            # get attributes with their dimensions from cube
            statistic_info = cube.metadata['statistic']
            statistic_key = statistic_info['name']
            cube_schema = CubeSchema(cube)
            attributes = cube_schema.attributes

            # prepare attributes
            for attribute_key, attribute_info in attributes.items():
                attribute_info['dimensions'] = cube_schema.dimensions
                attribute_info['region_levels'] = cube_schema.region_levels

            # add attributes to schema
            if statistic_key in schema:
                existing_attributes = schema[statistic_key]['attributes']
                if attribute_key not in existing_attributes:
                    existing_attributes[attribute_key] = attribute_info
                else:
                    existing_attributes[attribute_key]['values'] += attribute_info['values']
                    existing_attributes[attribute_key]['region_levels'] |= attribute_info['region_levels']
                    for k, v in attribute_info['dimensions'].items():
                        existing_attributes[attribute_key]['dimensions'][k] = v
            else:
                schema[statistic_key] = statistic_info
                schema[statistic_key]['attributes'] = attributes
        except KeyError:
            logger.warn('No metadata for cube `%s`' % cube.name)

    sys.stdout.write(json.dumps(schema, default=_dumper))
