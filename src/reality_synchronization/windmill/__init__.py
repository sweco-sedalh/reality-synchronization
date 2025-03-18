from typing import TypedDict, cast

import psycopg
import requests
import wmill

from reality_synchronization import make_oauth2_session


class postgresql(TypedDict):
    host: str
    port: int
    user: str
    dbname: str
    sslmode: str
    password: str
    root_certificate_pem: str


def connect_to_postgresql(db_config: postgresql) -> psycopg.Connection:
    return psycopg.connect(
        host=db_config["host"],
        port=db_config.get("port", 5432),
        user=db_config["user"],
        password=db_config["password"],
        dbname=db_config["dbname"],
        sslmode=db_config.get("sslmode", None),
        sslrootcert=db_config["root_certificate_pem"],
    )


class OAuth(TypedDict):
    client_id: str
    client_secret: str


def oauth2_client(
    resource_id: str,
    scope: str | None,
    token_url: str = "https://apimanager.lantmateriet.se/oauth2/token",
) -> requests.Session:
    config = cast(OAuth, wmill.get_resource(resource_id))
    return make_oauth2_session(
        config["client_id"], config["client_secret"], scope, token_url
    )
