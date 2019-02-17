import os
import asyncio
import argparse
from json import loads
from datetime import datetime, timedelta

import aiohttp
import aiofiles


JWT_TOKEN = os.environ.get("JWT_TOKEN", "")


class AuthenticationError(Exception):
    pass


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


class ApiEngine:
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    tasking_api_address = 'https://spaceknow-tasking.appspot.com/tasking/get-status'

    def __init__(self, api_address):
        self.api_address = api_address

    async def _make_request(self, address, payload):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    f'{address}',
                    headers=self.headers,
                    json=payload
            ) as resp:
                response = await resp.json()
                if resp.status != 200:
                    error = response.get('error', None)
                    if error == 'INVALID-AUTHORIZATION-HEADER':
                        raise AuthenticationError('Invalid token used')
                    else:
                        raise Exception(response.get('errorMessage', None))
                return response

    @staticmethod
    def _prepare_initiate_payload(geojson, days_ago=90):
        past_datetime = datetime.now() - timedelta(days=days_ago)

        request_payload = {
            "provider": "gbdx",
            "dataset": "idaho-pansharpened",
            "startDatetime": past_datetime.strftime("%Y-%m-%d 00:00:00"),
            "extent": geojson
        }
        return request_payload

    async def _initiate(self, geojson):
        initiate_payload = self._prepare_initiate_payload(geojson)
        return await self._make_request(f'{self.api_address}/initiate', initiate_payload)

    async def _retrieve(self):
        return await self._make_request(f'{self.api_address}/retrieve', {})

    async def _get_status(self, pipeline_id):
        return await self._make_request(self.tasking_api_address, {'pipelineId': pipeline_id})

    async def get_data(self, geojson):
        initiate_response = await self._initiate(geojson)
        next_try, pipeline_id = initiate_response.get('nextTry'), initiate_response.get('pipelineId')

        while True:
            await asyncio.sleep(next_try)
            status_response = await self._get_status(pipeline_id)
            status = status_response.get('status', '')
            if status == "RESOLVED":
                break
            if status == "FAILED":
                raise Exception(f"An error occurred during processing pipeline {pipeline_id}")
            next_try = status_response.get('nextTry', 100)

        return pipeline_id


async def main():
    file_path = get_file_path()

    geojson = await get_geojson(file_path)

    ragnar_search_api = ApiEngine('https://spaceknow-imagery.appspot.com/imagery/search')

    resp = await ragnar_search_api.get_data(geojson)
    print(resp)


if __name__ == "__main__":
    asyncio.run(main())
