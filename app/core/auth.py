import httpx
from async_lru import alru_cache
from fastapi import Depends, Header
from app.core.config import settings
from app.core.logging import logger



# @alru_cache(maxsize=128, ttl=300)  # cache for five minutes (300 seconds)
# async def fetch_userinfo(access_token: str):
#     url = f"{settings.ISSUER_URL}/oidc/v1/userinfo"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     async with httpx.AsyncClient(follow_redirects=True) as client:
#         resp = await client.get(url, headers=headers)
#         resp.raise_for_status()
#         return resp.json()


async def get_enhanced_user(
    # authorization: str = Header(..., alias="Authorization"),
) -> dict:
    ## TODO : Implement the whole logic for this function
    return {"sub": "user-101322","username":"default-user"}
