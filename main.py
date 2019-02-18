import os
import asyncio
import argparse
from json import loads
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import logging

import aiohttp
import aiofiles
from PIL import Image

io_pool_exc = ThreadPoolExecutor()

JWT_TOKEN = os.environ.get("JWT_TOKEN", "")


class AuthenticationError(Exception):
    pass


async def get_geojson(file_path):
    """
    Load geojson from file containing extent that you want to analyze
    :param file_path:
    :return: geojson dict
    """
    async with aiofiles.open(file_path, mode='r') as f:
        geojson = await f.read()
        return loads(geojson)


def get_file_path():
    """
    Get path of the geojson file as an script argument
    :return: Path to the file
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="enter path to the GeoJSON file", type=str)
    args = parser.parse_args()
    return args.file


class ApiEngine:
    """
    This class can be used with any asynchronous API endpoint that creates pipeline and needs some processing time to
    get data from retrieve endpoint. Use public get_data method.
    """
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
                logging.info(f"Request to {address} was successful")
                return response

    async def _initiate(self, initiate_payload):
        return await self._make_request(f'{self.api_address}/initiate', initiate_payload)

    async def _retrieve(self, pipeline_id):
        return await self._make_request(f'{self.api_address}/retrieve', {'pipelineId': pipeline_id})

    async def _get_status(self, pipeline_id):
        return await self._make_request(self.tasking_api_address, {'pipelineId': pipeline_id})

    async def get_data(self, initiate_payload):
        """
        Creates pipeline, waits until it is processed and returns response from retrieve endpoint
        :param initiate_payload: payload for specific /initiate endpoint
        :return: json data from retrieve endpoint
        """
        initiate_response = await self._initiate(initiate_payload)
        next_try, pipeline_id = initiate_response.get('nextTry'), initiate_response.get('pipelineId')

        while True:
            logging.info(f"Waiting {next_try} seconds for next {pipeline_id} pipeline id check")
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
    """
    This class implements all the methods needed to get scenes providing geojson polygon, to get map grids with ground
     and car visualized images, to create merged images with ground satellite image as a background and also to save
     these images to the drive.
    """
    def __init__(self):
        """
        Init all the API instances that are needed to get requested scenes and images
        """
        self.ragnar_search_api = ApiEngine('https://spaceknow-imagery.appspot.com/imagery/search')
        self.kraken_imagery_api = ApiEngine('https://spaceknow-kraken.appspot.com/kraken/release/imagery/geojson')
        self.kraken_cars_api = ApiEngine('https://spaceknow-kraken.appspot.com/kraken/release/cars/geojson')

    @staticmethod
    def _get_search_payload(geojson, days_ago):
        """
        Prepare and get payload for Ragnar API search endpoint
        :param geojson: Map extent specified in geojson format
        :param days_ago: Number of days ago, since you want to obtain scenes
        :return: payload
        """
        past_datetime = datetime.now() - timedelta(days=days_ago)

        request_payload = {
            "provider": "gbdx",
            "dataset": "idaho-pansharpened",
            "startDatetime": past_datetime.strftime("%Y-%m-%d 00:00:00"),
            "extent": geojson
        }
        return request_payload

    @staticmethod
    def _get_release_payload(geojson, scene_id):
        """
        :param geojson: Map extent specified in geojson format
        :param scene_id: id of the scene
        :return: payload
        """
        request_payload = {
            "sceneId": scene_id,
            "extent": geojson
        }
        return request_payload

    @staticmethod
    async def _get_file(session, map_id, tile_coordinates, file_type='truecolor.png'):
        """
        Get grid file from the address determined by all the parameters
        :param session: http session given to the method
        :param map_id: id of the map
        :param tile_coordinates: coordinates of the maps grid
        :param file_type: type of the supported file: truecolor.png/cars.png/area.json...
        :return: bytes file
        """
        async with session.get(
                f'https://spaceknow-kraken.appspot.com/kraken/grid/{map_id}/-/'
                f'{tile_coordinates[0]}/{tile_coordinates[1]}/{tile_coordinates[2]}/{file_type}'
        ) as resp:
            if resp.status != 200:
                response = await resp.json()
                raise Exception(response.get('errorMessage', None))
            return await resp.read()

    async def get_all_scenes(self, geojson, days_ago=90):
        """
        Get all available scenes not older then days_ago parameter, for the extent specified by geojson
        :param geojson: Map extent specified in geojson format
        :param days_ago: not older than
        :return: all obtained scenes
        """
        scenes = await self.ragnar_search_api.get_data(self._get_search_payload(geojson, days_ago))
        logging.info("Scenes from search api were obtained")
        return scenes

    @staticmethod
    def choose_best_scene(scenes):
        """
        Choose one from the obtained scenes. Decide based on visibility of the scene and chose the scene with the
        highest resolution.
        :param scenes: List of scenes
        :return: Id of the chosen scene
        """
        best_scene = None
        for scene in scenes['results']:
            if scene.get('cloudCover', 1) <= 0.30:
                if not best_scene:
                    best_scene = scene
                elif scene['bands'][0]['gsd'] < best_scene['bands'][0]['gsd']:
                    best_scene = scene

        best_scene = best_scene if best_scene else scenes['results'][0]
        logging.info(f"Scene {best_scene['sceneId']} was chosen")
        return best_scene['sceneId']

    async def get_scene_maps(self, geojson, scene_id):
        """
        Get both imagery and cars grid maps for the provided scene id
        :param geojson: Map extent specified in geojson format
        :param scene_id:
        :return: maps
        """
        imagery_map = await self.kraken_imagery_api.get_data(self._get_release_payload(geojson, scene_id))
        logging.info("Image map for imagery was obtained")

        cars_map = await self.kraken_cars_api.get_data(self._get_release_payload(geojson, scene_id))
        logging.info("Image map for cars was obtained")

        return imagery_map, cars_map

    async def save_map_images(self, imagery_map, cars_map):
        if not os.path.exists('./output'):
            os.makedirs('./output')
        async with aiohttp.ClientSession() as session:
            image_parts = []
            for tile in imagery_map['tiles']:
                background = await self._get_file(session, imagery_map['mapId'], tile)
                foreground = await self._get_file(session, cars_map['mapId'], tile, file_type='cars.png')
                image_parts.append((background, foreground))
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(io_pool_exc, self.merge_files, image_parts, f'output')

    @staticmethod
    def merge_files(image_parts, name):
        for background, foreground in image_parts:
            background_image = Image.open(BytesIO(background))
            foreground_image = Image.open(BytesIO(foreground))
            background_image.paste(foreground_image, (0, 0), foreground_image)
            background_image.save(f"./output/{name}.png", "PNG")


async def main():

    file_path = get_file_path()
    geojson = await get_geojson(file_path)

    car_visualizer = CarVisualizer()
    scenes = await car_visualizer.get_all_scenes(geojson)
    best_scene_id = car_visualizer.choose_best_scene(scenes)
    imagery_map, cars_map = await car_visualizer.get_scene_maps(geojson, best_scene_id)

    #imagery_map = {'mapId': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJtYXBJZCI6Ikd1b0JGcXR1QmxsR3FaV0hiMzk1Vlh5dEJndG5YS1Y3ZU1wRm9BMXRvZzV0UXVJQ1dTaFFFSTFjRlMxYzZYNE95QV9MM3lucXowMk9xN283IiwibWFwVHlwZSI6ImltYWdlcnkiLCJnZW9tZXRyeUlkIjoiNWZkNmM3ZDk2ZSIsInZlcnNpb24iOiIxNTIiLCJleHAiOjE1ODE5ODQ2NjQsInRpbGVzIjpbeyJ4Ijo2MDY0MCwieSI6Mzc5NTYsInpvb20iOjE2fSx7IngiOjYwNjM5LCJ5IjozNzk1NSwiem9vbSI6MTZ9LHsieCI6NjA2MzksInkiOjM3OTU2LCJ6b29tIjoxNn0seyJ4Ijo2MDY0MCwieSI6Mzc5NTUsInpvb20iOjE2fV19.K2YxlWUzdDFQjTOBpZi2B2PksRlcjEv7gVyn4aJE4ms', 'maxZoom': 19, 'tiles': [[16, 60640, 37956], [16, 60639, 37955], [16, 60639, 37956], [16, 60640, 37955]]}
    #cars_map = {'mapId': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJtYXBJZCI6Ikd1b0JGcXR1QmxsR3FaV0hiMzk1Vlh5dEJndG5YS1Y3ZU1wRm9BMXRvZzV0UXVJQ1dTaFFFSTFjRlMxYzZYNE95QV9MM3lucXowMk9xN283IiwibWFwVHlwZSI6ImNhcnMiLCJnZW9tZXRyeUlkIjoiNWZkNmM3ZDk2ZSIsInZlcnNpb24iOiIxNTguMCIsImV4cCI6MTU4MTk4NDY3MSwidGlsZXMiOlt7IngiOjYwNjM5LCJ5IjozNzk1Niwiem9vbSI6MTZ9LHsieCI6NjA2NDAsInkiOjM3OTU2LCJ6b29tIjoxNn0seyJ4Ijo2MDY0MCwieSI6Mzc5NTUsInpvb20iOjE2fSx7IngiOjYwNjM5LCJ5IjozNzk1NSwiem9vbSI6MTZ9XX0.UwPbVDEJCyKhgTchHchfYvZTGAGARlfjNFZw0wAXB1s', 'maxZoom': 19, 'tiles': [[16, 60639, 37956], [16, 60640, 37956], [16, 60640, 37955], [16, 60639, 37955]]}
    await car_visualizer.save_map_images(imagery_map, cars_map)


if __name__ == "__main__":
    asyncio.run(main())
