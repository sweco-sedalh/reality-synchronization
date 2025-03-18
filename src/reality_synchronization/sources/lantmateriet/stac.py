import logging
import os
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from geopandas import GeoDataFrame
from pyogrio import read_dataframe, list_layers
from pystac import Item
from pystac_client import Client
from requests import Session

from reality_synchronization import make_oauth2_session


logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    remote_updated: datetime | None
    layers: dict[str, GeoDataFrame]


class Loader:
    scope: str | None

    def load(self, subdivision: str, session: Session) -> LoadResult:
        raise NotImplementedError

    def last_updated(self, subdivision: str, session: Session) -> datetime | None:
        raise NotImplementedError


def load_remote_zip(url: str, session: Session) -> dict[str, GeoDataFrame]:
    logger.info("Downloading %s", url)
    response = session.get(url, stream=True)
    with tempfile.TemporaryDirectory() as tmpdirname:
        zip_path = f"{tmpdirname}/data.zip"
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
        logger.info("Extracting zip")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdirname)
        file = next(
            f
            for f in Path(tmpdirname).iterdir()
            if f.is_file() and f.name != "data.zip"
        )
        logger.info("Loading data")
        result = {}
        for (layer, geometry_type) in list_layers(file):
            logger.info("Loading layer %s", layer)
            result[layer] = read_dataframe(file, layer=layer)
        return result


class LantmaterietStacLoader(Loader):
    domain: str

    @classmethod
    def _get_item(cls, municipality_code: str | Item) -> Item:
        if isinstance(municipality_code, Item):
            return municipality_code
        stac_client = Client.open("https://api.lantmateriet.se/stac-vektor/v1/")
        return stac_client.get_collection(cls.domain).get_item(municipality_code)

    def load(self, municipality_code: str | Item, session: Session) -> LoadResult:
        item = self._get_item(municipality_code)
        href = item.assets["data"].href
        return LoadResult(
            layers=load_remote_zip(href, session),
            remote_updated=item.common_metadata.updated,
        )

    def last_updated(
        self, municipality_code: str | Item, session: Session
    ) -> datetime | None:
        item = self._get_item(municipality_code)
        return item.common_metadata.updated


class FastighetsindelningLoader(LantmaterietStacLoader):
    scope = "ogc-features:fastighetsindelning.read"
    domain = "fastighetsindelning"


class BelagenhetsadressLoader(LantmaterietStacLoader):
    scope = None
    domain = "belagenhetsadress"


class ByggnaderLoader(LantmaterietStacLoader):
    scope = None
    domain = "byggnader"


class MarktackeLoader(LantmaterietStacLoader):
    scope = "ogc-features:marktacke.read"
    domain = "marktacke"


class OrtnamnLoader(LantmaterietStacLoader):
    scope = None
    domain = "ortnamn"


class KommunLanRikeLoader(LantmaterietStacLoader):
    scope = None
    domain = "kommun-lan-rike"
