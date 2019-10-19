"""
the store manages cubes data on disk, download from webservices and export
to json facts

it allows partial updates (when cubes changes)

every information is stored in the filesystem so there is no need for an
extra database to keep track of the status of the cubes

a `Storage` has a base directory with this layout:

./
    webservice_url                  -   plain text file containing the webservice url used
    last_updated                    -   plain text file containing date in isoformat
    last_exported                   -   plain text file containing date in isoformat
    logs/                           -   folder for keeping logfiles
    11111BJ001/                     -   directory for cube name "11111BJ001"
        last_updated                -   plain text file containing date in isoformat
        last_exported               -   plain text file containing date in isoformat
        current/                    -   symbolic link to the latest revision directory
        2019-08-07T08:40:20/        -   revision directory for given date (isoformat)
            downloaded              -   plain text file containing date in isoformat
            exported                -   plain text file containing date in isoformat
            meta.yml                -   original metadata from webservice in yaml format
            data.csv                -   original csv data for this cube
        2017-06-07T08:40:20/        -   an older revision...
            ...
    11111BJ002/                     -   another cube...
        ...


"""

import logging
import os
import re
import yaml

from datetime import datetime
from regenesis.cube import Cube as RegenesisCube

from genesapi.exceptions import StorageDoesNotExist, ShouldNotHappen
from genesapi.soap_services import IndexService, ExportService
from genesapi.util import get_value_from_file, to_date, is_isoformat


logger = logging.getLogger(__name__)


CUBE_NAME_RE = re.compile(r'^\d{5}[A-Z]')  # FIXME


class Mixin:
    @property
    def last_exported(self):
        return get_value_from_file(self._path('last_exported'), transform=to_date)

    @property
    def last_updated(self):
        return get_value_from_file(self._path('last_updated'), transform=to_date)

    def _path(self, *paths):
        return os.path.join(self.directory, *paths)

    def touch(self, item):
        # write current time into `item` (which is a file path)
        with open(self._path(item), 'w') as f:
            f.write(datetime.now().isoformat())

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.directory)


class CubeRevision(Mixin):
    def __init__(self, cube, name):
        self.cube = cube
        self.name = name
        self.date = to_date(name)
        self.directory = os.path.join(cube.directory, name)
        self.exists = os.path.exists(self.directory)

    @property
    def downloaded(self):
        return get_value_from_file(self._path('downloaded'), transform=to_date)

    @property
    def exported(self):
        return get_value_from_file(self._path('downloaded'), transform=to_date)

    @property
    def metadata(self):
        return get_value_from_file(self._path('meta.yml'), transform=yaml.load)

    def create(self, download_metadata, cube_metadata, cube_data, overwrite=False):
        logger.log(logging.INFO, 'Creating new revision for cube `%s` ...' % self.cube)
        if overwrite:
            logger.log(logging.INFO, '(Force updating)')
        if self.exists and not overwrite:
            raise ShouldNotHappen(
                'Revision "%s" for cube "%s" already exists!' %
                (self.cube.name, self.date.isoformat()))
        os.makedirs(self.directory, exist_ok=True)
        self.touch('downloaded')
        with open(self._path('download.yml'), 'w') as f:
            f.write(yaml.dump(download_metadata, default_flow_style=False))
        with open(self._path('meta.yml'), 'w') as f:
            f.write(yaml.dump(cube_metadata, default_flow_style=False))
        with open(self._path('data.csv'), 'w') as f:
            f.write(cube_data)

        # update current symlink
        fp = self.cube._path('current')
        if os.path.exists(fp):
            os.remove(fp)
        os.symlink(self.name, fp)
        logger.log(logging.INFO, 'Created new revision `%s` for cube `%s`.' % (self.name, self.cube))

    def load(self):
        with open(self._path('data.csv')) as f:
            raw = f.read().strip()
        return RegenesisCube(self.cube.name, raw)


class Cube(Mixin):
    def __init__(self, name, storage):
        self.name = name
        self.storage = storage
        self.directory = storage._path(name)
        self.exists = os.path.exists(self.directory)

    @property
    def current(self):
        return self.revisions[0]

    @property
    def metadata(self):
        return get_value_from_file(self._path('current', 'meta.yml'), transform=yaml.load)

    @property
    def facts(self):
        return self.current.load().facts

    @property
    def revisions(self):
        return sorted([CubeRevision(self, rev) for rev in os.listdir(self.directory) if is_isoformat(rev)],
                      key=lambda x: x.date, reverse=True)

    def should_update(self, date=None):
        if not self.exists:
            logger.log(logging.INFO, 'Updating cube `%s` because it didn\'t exist yet ...' % self.name)
            return True
        if date is None:
            cube_metadata = IndexService.get_metadata_for_cube(self.name)
            date = to_date(cube_metadata['stand'], force_ws=True)
        should_update = self.current.date < date
        if should_update:
            logger.log(logging.INFO, 'Updating cube `%s` because a newer version is available ...' % self.name)
        else:
            logger.log(logging.INFO, 'Cube `%s` is up to date.' % self.name)
        return should_update

    def update(self, force=False):
        if force or self.should_update():
            download_metadata, cube_metadata, cube_data = ExportService.download_cube(self.name)
            if cube_metadata['stand'] and cube_data:
                rev_name = to_date(cube_metadata['stand'], force_ws=True).isoformat()
                revision = CubeRevision(self, rev_name)
                revision.create(download_metadata, cube_metadata, cube_data, force)
                self.touch('last_updated')
            else:
                logger.log(logging.ERROR, 'Cube `%s` seems not to be valid' % self)

    def should_export(self, force=False):
        if force:
            return True
        if self.last_exported:
            return self.last_updated > self.last_exported
        return True

    def export(self, force=False):
        if force or self.should_export():
            self.touch('last_exported')
            return self.current.load()


class Storage(Mixin):
    def __init__(self, directory):
        if not os.path.exists(directory):
            raise StorageDoesNotExist(
                'Storage does not exist at `%s`. If you want to create it, use `Storage.create("%s")`' %
                (directory, directory))
        self.directory = directory
        self.name = directory
        self.loggingHandler = logging.FileHandler(self._path('logs', '%s.log' % datetime.now().isoformat()))
        self.loggingHandler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))

    def __iter__(self):
        for fp in os.listdir(self.directory):
            if CUBE_NAME_RE.match(fp):
                yield Cube(fp, self)

    def update(self, prefix=None, force=False):
        logger.addHandler(self.loggingHandler)
        if prefix:
            entries = IndexService.filter(prefix)
        else:
            entries = IndexService
        for entry in entries:
            cube = Cube(entry['code'], self)
            cube.update(force)
        self.touch('last_updated')

    def get_cubes_for_export(self, force=False):
        return [c for c in self if c.should_export(force)]

    @property
    def webservice_url(self):
        return get_value_from_file(self.path('webservice_url'))

    @classmethod
    def create(cls, directory):
        os.mkdir(directory)
        os.mkdir(os.path.join(directory, 'logs'))
        return Storage(directory)
