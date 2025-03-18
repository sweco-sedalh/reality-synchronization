import logging
from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods
from pypgstac.migrate import Migrate
from pystac_client import Client
from typing import Literal
from wmill import set_progress

from reality_synchronization.windmill import postgresql

logger = logging.getLogger(__name__)


def fetch_lantmateriet_stac_items(
        catalog: Literal["bild", "hojd", "vektor"], database: postgresql
):
    client = Client.open(f"https://api.lantmateriet.se/stac-{catalog}/v1/")

    logger.info("Preparing database")
    db = PgstacDB(
        dsn=f"postgresql://{database['user']}:{database['password']}@{database['host']}:{database.get('port', 5432)}/{database['dbname']}")
    Migrate(db).run_migration()

    loader = Loader(db)

    logger.info("Downloading collections")
    collections = [
        {**c.to_dict(), "id": "lantmateriet/" + c.id.replace("orto-", "orto/")}
        for c in client.get_collections()
    ]

    set_progress(10)

    logger.info("Upserting collections into database")
    loader.load_collections(collections, Methods.upsert)

    set_progress(20)

    logger.info("Downloading items")
    with db as db:
        db.connection.add_notice_handler(lambda a: logger.info(repr(a)))
        for idx, page in enumerate(client.search(limit=10000).pages_as_dicts()):
            logger.info("Processing page %s", idx)
            items = [
                {
                    **i,
                    "collection": "lantmateriet/" + i["collection"].replace("orto-", "orto/")
                }
                for i in page.get("features", [])
            ]
            loader.load_items(items, Methods.upsert)
        db.connection.commit()

    return dict(collections=collections)
