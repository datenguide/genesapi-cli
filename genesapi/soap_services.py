import logging
import os
import yaml
from zeep import Client, Settings

from genesapi.exceptions import UndefinedCatalog, UnexpectedSoapResult


logger = logging.getLogger(__name__)


class BaseService:
    def __init__(self):
        catalog = os.getenv('CATALOG')
        if catalog is None:
            raise UndefinedCatalog('Please specify a path to the catalog.yaml via `CATALOG` env var')

        logger.log(logging.DEBUG, 'Using `%s` as catalog' % catalog)

        with open(catalog) as f:
            catalog = yaml.load(f.read().strip())

        self.client = Client(catalog['%s_url' % self.__class__.__name__.lower()],
                             settings=Settings(strict=False, xml_huge_tree=True))
        self.kwargs = {
            'kennung': catalog['username'],
            'passwort': catalog['password'],
            'bereich': 'Alle',
            'sprache': 'de'  # FIXME
        }

    def to_dict(self, element):
        return {e.tag: e.text for e in element}


class Index(BaseService):
    def __init__(self):
        super().__init__()
        self.service = self.client.service.DatenKatalog
        self.kwargs.update(listenLaenge=500)

    def get_metadata_for_cube(self, cube_name):
        logger.log(logging.DEBUG, 'Obtaining metadata for cube `%s` ...' % cube_name)
        res = self.service(filter=cube_name, **self.kwargs)
        if len(res.datenKatalogEintraege) > 1:
            raise UnexpectedSoapResult('Got more than 1 cube')
        data = res.datenKatalogEintraege[0]
        return self.to_dict(data)

    def filter(self, prefix):
        logger.log(logging.INFO, 'Look up cubes with name starting with `%s` ...' % prefix)
        res = self.service(filter='%s*' % prefix, **self.kwargs)
        if len(res.datenKatalogEintraege) == 500:
            raise UnexpectedSoapResult('Cube list for "%s*" too long' % prefix)
        logger.log(logging.INFO, 'Found %s cubes with name starting with `%s`' %
                   (len(res.datenKatalogEintraege), prefix))
        return [self.to_dict(e) for e in res.datenKatalogEintraege]

    def __iter__(self):
        for i in range(10, 100):
            for entry in self.filter(i):
                yield entry


class Export(BaseService):
    def __init__(self):
        super().__init__()
        self.service = self.client.service.DatenExport
        self.kwargs.update(
            format='csv',
            werte=True,
            metadaten=True,
            zusatz=True,
            startjahr=1990,
            endjahr='',
            zeitscheiben='',
            inhalte='',
            regionalmerkmal='',
            regionalschluessel='',
            sachmerkmal='',
            sachschluessel='',
            sachmerkmal2='',
            sachschluessel2='',
            sachmerkmal3='',
            sachschluessel3='',
            stand=''
        )

    def download_cube(self, name):
        logger.log(logging.INFO, 'Downloading cube `%s` from `%s` ...' % (name, self.client.wsdl.location))
        res = self.service(namen=name, **self.kwargs)
        download_metadata = {k: getattr(res, k) for k in res if k != 'quader'}
        cube = res.quader[0]
        cube_metadata = {
            e.tag: e.text for e in cube
            if e.tag != 'quaderDaten'
        }
        cube_data = cube.find('quaderDaten').text
        logger.log(logging.INFO, 'Downloaded cube `%s`.' % name)
        return download_metadata, cube_metadata, cube_data


IndexService = Index()
ExportService = Export()
