import asyncio
import os
import aiohttp
from dotenv import load_dotenv

async def main():
    load_dotenv()
    token = os.getenv("OWS_TOKEN")
    if not token:
        print("OWS_TOKEN not found")
        return

    async with aiohttp.ClientSession() as session:
        # Check current cursor (15997176)
        print("Checking default list (latest):")
        async with session.get("https://ows.goszakup.gov.kz/v3/trd-buy?limit=1", headers={"Authorization": f"Bearer {token}"}) as resp:
            data = await resp.json()
            if data['items']:
                print(f"Latest ID: {data['items'][0]['id']}, Date: {data['items'][0]['publishDate']}")
            else:
                print("No latest items")

        # Check cursor from DB
        cursor = "https://ows.goszakup.gov.kz/v3/trd-buy?page=next&search_after=15997176"
        print(f"\nChecking cursor: {cursor}")
        async with session.get(cursor, headers={"Authorization": f"Bearer {token}"}) as resp:
            data = await resp.json()
            if 'items' in data and data['items']:
                first_item = data['items'][0]
                print(f"Cursor Item ID: {first_item['id']}, Date: {first_item['publishDate']}")
            else:
                print("Cursor empty or invalid")

if __name__ == "__main__":
    asyncio.run(main())
