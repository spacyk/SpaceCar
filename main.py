import os
import asyncio
import argparse
from json import loads

import aiohttp
import aiofiles


JWT_TOKEN = os.environ.get("JWT_TOKEN", "")

async def test_request(json_data):
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    async with aiohttp.ClientSession() as session:
        async with session.post('https://spaceknow-credits.appspot.com/credits/area/allocate-geojson', headers=headers, json=json_data) as resp:
            print(resp.status)
            return await resp.json()


async def get_geojson(filepath):
    async with aiofiles.open(filepath, mode='r') as f:
        geojson = await f.read()
        return loads(geojson)


def get_file_path():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="enter path to the GeoJSON file",
                        type=str)
    args = parser.parse_args()
    return args.file


def get_request_payload(geojson):
    request_payload = {
        "ranges": [
        {
            "from": "2016-05",
            "to": "2017-06"
        }
        ],
        "geojson": geojson
    }

    return request_payload


async def main():
    file_path = get_file_path()

    geojson = await get_geojson(file_path)

    json_payload = get_request_payload(geojson)

    response = await test_request(json_payload)
    print(response)


if __name__ == "__main__":
    asyncio.run(main())
