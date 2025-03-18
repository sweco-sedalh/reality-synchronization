import logging

from pathlib import Path

import zipfile

import tempfile

from geopandas import GeoDataFrame
from pyogrio import list_layers, read_dataframe
from typing import Callable

from requests import Session


logger = logging.getLogger(__name__)


def load_remote_zip(url: str, session: Session, postprocess: Callable[[str, GeoDataFrame], GeoDataFrame]) -> dict[str, GeoDataFrame]:
    logger.info("Downloading %s", url)
    response = session.get(url, stream=True)
    response.raise_for_status()
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
            df = read_dataframe(file, layer=layer, use_arrow=True)
            if df.index is not None and not df.index.name:
                df = df.reset_index(drop=True)
            df = postprocess(layer, df)
            result[layer] = df
        return result
