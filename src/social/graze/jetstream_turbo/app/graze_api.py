import httpx
from typing import List
from social.graze.jetstream_turbo.app.config import Settings


class GrazeAPI:
    """
    Fetch credentials from the Graze Social API and extract session strings.
    """

    @staticmethod
    async def fetch_session_strings(settings: Settings) -> List[str]:
        url = f"{settings.graze_api_base_url.rstrip('/')}/app/api/v1/turbo-tokens/credentials?credential_secret={settings.turbo_credential_secret}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            credentials = response.json()
        return [cred["session_string"] for cred in credentials]
