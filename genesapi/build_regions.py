"""
build name mapping for regions

{
    "08425": {
        "id": "08425", // AGS for the region
        "name": "Alb-Donau-Kreis", // Nicely formated name of the region
        "type": "Landkreis", // Type of region (e.g. Kreisfreie Stadt, Regierungsbezirk)
        "level": 3, // NUTS level (1-3), LAU (4)
        "duration": {
            "from": "2012-01-01", // ISO dates for earliest available statistical measure
            "until": "2019-12-31"  // ISO dates for latest available statistical measure
        }
    },
}
"""


import json
import logging
import sys

from genesapi.storage import Storage
from genesapi.util import time_to_json


logger = logging.getLogger(__name__)


def main(args):
    storage = Storage(args.storage)
    names = {}
    for cube in storage:
        logger.info('Loading `%s` ...' % cube.name)
        min_date, max_date = cube.schema.data_date_range
        for region_id, region in cube.schema.regions.items():
            if region_id in names:
                _min_date = names[region_id]['duration']['from']
                _max_date = names[region_id]['duration']['until']
                names[region_id]['duration']['from'] = min(min_date, _min_date)
                names[region_id]['duration']['until'] = max(max_date, _max_date)
            else:
                names[region_id] = {**region, **{
                    'duration': {
                        'from': min_date,
                        'until': max_date
                    }
                }}

    sys.stdout.write(json.dumps(names, default=time_to_json))
