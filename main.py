import os
import asyncio
import argparse
from json import loads
from datetime import datetime

import aiohttp
import aiofiles


JWT_TOKEN = os.environ.get("JWT_TOKEN", "")


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


async def test_request(json_data):
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    async with aiohttp.ClientSession() as session:
        async with session.post('https://spaceknow-credits.appspot.com/credits/area/allocate-geojson', headers=headers, json=json_data) as resp:
            print(resp.status)
            return await resp.json()


async def tasking_request(pipeline_id):
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    async with aiohttp.ClientSession() as session:
        async with session.post('https://spaceknow-tasking.appspot.com/tasking/get-status', headers=headers, json=dict(pipelineId=pipeline_id)) as resp:
            print(resp.status)
            return await resp.json()


class RagnarApiEngine:
    address = 'https://spaceknow-imagery.appspot.com'

    def __init__(self, geojson):
        self.datetime = datetime.now().strftime("%Y-%m-%d 00:00:00")
        self.geojson = geojson
        self.search_payload = self.get_search_payload()

    async def initiate_search(self):
        headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    f'{self.address}/imagery/search/initiate',
                    headers=headers,
                    json=self.search_payload
            ) as resp:
                print(resp.status)
                return await resp.json()

    def get_search_payload(self):
        request_payload = {
            "provider": "gbdx",
            "dataset": "idaho-pansharpened",
            "startDatetime": self.datetime,
            "extent": self.geojson
        }

        return request_payload


async def main():
    file_path = get_file_path()

    geojson = await get_geojson(file_path)

    #json_payload = get_request_payload(geojson)
    #response = await test_request(json_payload)
    #print(response)

    ragnar_api = RagnarApiEngine(geojson)

    resp = ragnar_api.initiate_search()
    print(resp)


if __name__ == "__main__":
    asyncio.run(main())
