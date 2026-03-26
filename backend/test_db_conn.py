import asyncio
import asyncpg
import sys

URL = "postgresql://postgres:2FG7cPMV5he3gPLh@db.iwzhrlorujsslilnbrcs.supabase.co:5432/postgres"

async def test_connection():
    try:
        print(f"Attempting to connect to Supabase...")
        conn = await asyncpg.connect(URL)
        print("Successfully connected!")
        version = await conn.fetchval('SELECT version();')
        print(f"PostgreSQL version: {version}")
        await conn.close()
        sys.exit(0)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(test_connection())
