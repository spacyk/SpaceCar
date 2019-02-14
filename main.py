import asyncio

import aiohttp


async def test_request():
    async with aiohttp.ClientSession() as session:
        async with session.get('http://httpbin.org/get') as resp:
            print(resp.status)
            print(await resp.text())

if __name__ == "__main__":
    asyncio.run(test_request())