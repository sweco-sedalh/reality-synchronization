import geopandas
import requests
from geopandas import GeoDataFrame

from reality_synchronization.util.load_remote_zip import load_remote_zip


def load_aro():
    url = "https://opendata-download.smhi.se/svar/SVAR2022_delavrinningsomraden.zip"

    def postprocess(layer: str, df: GeoDataFrame) -> GeoDataFrame:
        return df.set_index("ARO_UUID")

    with requests.Session() as session:
        return load_remote_zip(url, session, postprocess)


def load_haro():
    df = geopandas.read_file("https://opendata-view.smhi.se/SMHI_vatten_RiverBasin/HY.PhysicalWaters.Catchments/wfs?service=wfs&request=getfeature&typeNames=SMHI_vatten_RiverBasin:HY.PhysicalWaters.Catchments&outputFormat=json")
    df = df.set_index("HARO")
    return df
