from typing import Literal

from psycopg.types.json import Jsonb
from pystac_client import Client
from wmill import set_progress

from reality_synchronization.windmill import postgresql

from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate

import logging

logger = logging.getLogger(__name__)


def fetch_lantmateriet_stac_items(
        catalog: Literal["bild", "hojd", "vektor"], database: postgresql
):
    client = Client.open(f"https://api.lantmateriet.se/stac-{catalog}/v1/")

    logger.info("Preparing database")
    db = PgstacDB(
        dsn=f"postgresql://{database['user']}:{database['password']}@{database['host']}:{database.get('port', 5432)}/{database['dbname']}")
    Migrate(db).run_migration()

    logger.info("Downloading collections")
    collections = [
        {**c.to_dict(), "id": "lantmateriet/" + c.id.replace("orto-", "orto/")}
        for c in client.get_collections()
    ]

    set_progress(10)

    logger.info("Upserting collections into database")
    with db as db:
        with db.connection.cursor() as cur:
            cur.executemany(
                "SELECT pgstac.upsert_collection(%s)",
                [(Jsonb(c),) for c in collections],
            )

    set_progress(20)

    logger.info("Downloading items")
    with db as db:
        for idx, page in enumerate(client.search(limit=10000).pages_as_dicts()):
            logger.info("Processing page %s", idx)
            items = [
                {
                    **i,
                    "collection": "lantmateriet/" + i["collection"].replace("orto-", "orto/")
                }
                for i in page.get("features", [])
            ]

            db.func("upsert_items", Jsonb(items))

    return dict(collections=collections)
