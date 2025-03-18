from typing import Any

from pystac_client.stac_api_io import StacApiIO


class HttpxStacApiIO(StacApiIO):
    def request(self,
        href: str,
        method: str | None = None,
        headers: dict[str, str] | None = None,
        parameters: dict[str, Any] | None = None,) -> str:
        pass
