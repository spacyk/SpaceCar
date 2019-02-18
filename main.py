import os
import asyncio
import argparse
from json import loads
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

import aiohttp
import aiofiles
from PIL import Image

io_pool_exc = ThreadPoolExecutor()

JWT_TOKEN = os.environ.get("JWT_TOKEN", "")


class AuthenticationError(Exception):
    pass


async def get_geojson(file_path):
    async with aiofiles.open(file_path, mode='r') as f:
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
                    address,
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

    async def _initiate(self, initiate_payload):
        return await self._make_request(f'{self.api_address}/initiate', initiate_payload)

    async def _retrieve(self, pipeline_id):
        return await self._make_request(f'{self.api_address}/retrieve', {'pipelineId': pipeline_id})

    async def _get_status(self, pipeline_id):
        return await self._make_request(self.tasking_api_address, {'pipelineId': pipeline_id})

    async def get_data(self, initiate_payload):
        initiate_response = await self._initiate(initiate_payload)
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

        retrieve_response = await self._retrieve(pipeline_id)

        return retrieve_response


class CarVisualizer:
    def __init__(self):
        self.ragnar_search_api = ApiEngine('https://spaceknow-imagery.appspot.com/imagery/search')
        self.kraken_imagery_api = ApiEngine('https://spaceknow-kraken.appspot.com/kraken/release/imagery/geojson')
        self.kraken_cars_api = ApiEngine('https://spaceknow-kraken.appspot.com/kraken/release/cars/geojson')

    @staticmethod
    def get_best_scene(scenes):
        best_scene = None
        for scene in scenes['results']:
            if scene.get('cloudCover', 1) <= 0.30:
                if not best_scene:
                    best_scene = scene
                elif scene['bands'][0]['gsd'] < best_scene['bands'][0]['gsd']:
                    best_scene = scene
        return best_scene if best_scene else scenes['results'][0]

    @staticmethod
    def get_search_payload(geojson, days_ago=90):
        past_datetime = datetime.now() - timedelta(days=days_ago)

        request_payload = {
            "provider": "gbdx",
            "dataset": "idaho-pansharpened",
            "startDatetime": past_datetime.strftime("%Y-%m-%d 00:00:00"),
            "extent": geojson
        }
        return request_payload

    @staticmethod
    def get_release_payload(geojson, scene_id):
        request_payload = {
            "sceneId": scene_id,
            "extent": geojson
        }
        return request_payload

    async def get_scene_maps(self, geojson):
        resp = await self.ragnar_search_api.get_data(self.get_search_payload(geojson))
        best_scene = self.get_best_scene(resp)
        imagery_map = await self.kraken_imagery_api.get_data(self.get_release_payload(geojson, best_scene['sceneId']))
        cars_map = await self.kraken_cars_api.get_data(self.get_release_payload(geojson, best_scene['sceneId']))

        return imagery_map, cars_map

    async def save_map_images(self, imagery_map, cars_map):
        if not os.path.exists('./output'):
            os.makedirs('./output')
        async with aiohttp.ClientSession() as session:
            for tile in imagery_map['tiles']:
                async with session.get(f'https://spaceknow-kraken.appspot.com/kraken/grid/{imagery_map["mapId"]}/-/{tile[0]}/{tile[1]}/{tile[2]}/truecolor.png') as resp:
                    if resp.status != 200:
                        response = await resp.json()
                        # Maybe only console log is better
                        raise Exception(response.get('errorMessage', None))
                    background = await resp.read()
                async with session.get(f'https://spaceknow-kraken.appspot.com/kraken/grid/{cars_map["mapId"]}/-/{tile[0]}/{tile[1]}/{tile[2]}/cars.png') as resp:
                    if resp.status != 200:
                        response = await resp.json()
                        raise Exception(response.get('errorMessage', None))
                    foreground = await resp.read()
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(io_pool_exc, self.merge_files, background, foreground, f'{tile[1]}{tile[2]}')

    @staticmethod
    def merge_files(background, foreground, name):
        background_image = Image.open(BytesIO(background))
        foreground_image = Image.open(BytesIO(foreground))
        background_image.paste(foreground_image, (0, 0), foreground_image)
        background_image.save(f"./output/{name}.png", "PNG")

async def main():

    file_path = get_file_path()
    geojson = await get_geojson(file_path)

    car_visualizer = CarVisualizer()

    imagery_map, cars_map = await car_visualizer.get_scene_maps(geojson)

    #imagery_map = {'mapId': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJtYXBJZCI6Ikd1b0JGcXR1QmxsR3FaV0hiMzk1Vlh5dEJndG5YS1Y3ZU1wRm9BMXRvZzV0UXVJQ1dTaFFFSTFjRlMxYzZYNE95QV9MM3lucXowMk9xN283IiwibWFwVHlwZSI6ImltYWdlcnkiLCJnZW9tZXRyeUlkIjoiNWZkNmM3ZDk2ZSIsInZlcnNpb24iOiIxNTIiLCJleHAiOjE1ODE5ODQ2NjQsInRpbGVzIjpbeyJ4Ijo2MDY0MCwieSI6Mzc5NTYsInpvb20iOjE2fSx7IngiOjYwNjM5LCJ5IjozNzk1NSwiem9vbSI6MTZ9LHsieCI6NjA2MzksInkiOjM3OTU2LCJ6b29tIjoxNn0seyJ4Ijo2MDY0MCwieSI6Mzc5NTUsInpvb20iOjE2fV19.K2YxlWUzdDFQjTOBpZi2B2PksRlcjEv7gVyn4aJE4ms', 'maxZoom': 19, 'tiles': [[16, 60640, 37956], [16, 60639, 37955], [16, 60639, 37956], [16, 60640, 37955]]}
    #cars_map = {'mapId': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJtYXBJZCI6Ikd1b0JGcXR1QmxsR3FaV0hiMzk1Vlh5dEJndG5YS1Y3ZU1wRm9BMXRvZzV0UXVJQ1dTaFFFSTFjRlMxYzZYNE95QV9MM3lucXowMk9xN283IiwibWFwVHlwZSI6ImNhcnMiLCJnZW9tZXRyeUlkIjoiNWZkNmM3ZDk2ZSIsInZlcnNpb24iOiIxNTguMCIsImV4cCI6MTU4MTk4NDY3MSwidGlsZXMiOlt7IngiOjYwNjM5LCJ5IjozNzk1Niwiem9vbSI6MTZ9LHsieCI6NjA2NDAsInkiOjM3OTU2LCJ6b29tIjoxNn0seyJ4Ijo2MDY0MCwieSI6Mzc5NTUsInpvb20iOjE2fSx7IngiOjYwNjM5LCJ5IjozNzk1NSwiem9vbSI6MTZ9XX0.UwPbVDEJCyKhgTchHchfYvZTGAGARlfjNFZw0wAXB1s', 'maxZoom': 19, 'tiles': [[16, 60639, 37956], [16, 60640, 37956], [16, 60640, 37955], [16, 60639, 37955]]}
    await car_visualizer.save_map_images(imagery_map, cars_map)


if __name__ == "__main__":
    asyncio.run(main())
