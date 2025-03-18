import logging
from typing import Type

from pystac import Item
from wmill import set_progress

from reality_synchronization.sinks.postgis import (
    write_postgis,
    create_metadata_table,
    upsert_metadata,
)
from reality_synchronization.sources.lantmateriet.stac import (
    FastighetsindelningLoader,
    LantmaterietStacLoader,
    ByggnaderLoader,
    BelagenhetsadressLoader,
    MarktackeLoader,
    KommunLanRikeLoader,
    OrtnamnLoader,
)
from reality_synchronization.windmill import (
    connect_to_postgresql,
    oauth2_client,
    postgresql,
)

LOADERS: dict[str, Type[LantmaterietStacLoader]] = {
    "fastighetsindelning": FastighetsindelningLoader,
    "byggnader": ByggnaderLoader,
    "belagenhetsadresser": BelagenhetsadressLoader,
    "marktacke": MarktackeLoader,
    "kommun-lan-rike": KommunLanRikeLoader,
    "ortnamn": OrtnamnLoader,
}

logger = logging.getLogger(__name__)


def fetch_lantmateriet_stac(item: dict, database: postgresql, oauth_resource_id: str):
    item = Item.from_dict(item)
    loader = LOADERS[item.collection_id]()

    with connect_to_postgresql(database) as db:
        create_metadata_table("data", db)

        with oauth2_client(oauth_resource_id, loader.scope) as session:
            data = loader.load(item, session)

            for idx, (layer, df) in enumerate(data.layers.items()):
                set_progress(int(50 + 50.0 * idx / len(data.layers)))
                table = f"{item.collection_id}_{layer}"
                if df.index is None or df.index.name is None:
                    logger.info("Ignoring layer %s as it does not have a named index", layer)
                else:
                    logger.info("Writing %d rows to %s", len(df), table)
                    if layer == "granspunkt":
                        duplicates = df.index.duplicated()
                        if duplicates.any():
                            df = df.reset_index().drop_duplicates(subset=["objektidentitet"]).set_index("objektidentitet")
                            logger.warning("Layer %s had %d duplicates", layer, duplicates.sum())

                    write_postgis(
                        table,
                        "data",
                        "temporary_data",
                        df,
                        db,
                        item.id,
                    )

                logger.info("Updating metadata for layer %s", layer)
                upsert_metadata(
                    table,
                    f"{item.collection_id}/{layer}",
                    layer,
                    "Lantmäteriet",
                    data.remote_updated,
                    "data",
                    item.self_href,
                    db,
                )

            return dict(layers={k: len(v) for k, v in data.layers.items()}, last_updated=data.remote_updated)
