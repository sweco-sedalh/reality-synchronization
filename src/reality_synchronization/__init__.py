import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session


def make_oauth2_session(
    client_id: str,
    client_secret: str,
    scope: str | None,
    token_url: str = "https://apimanager.lantmateriet.se/oauth2/token",
):
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client, scope=scope)
    token = oauth.fetch_token(
        token_url, client_id=client_id, client_secret=client_secret
    )
    session = requests.Session()
    session.headers.update({"Authorization": "Bearer " + token["access_token"]})
    return session
