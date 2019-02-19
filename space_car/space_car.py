import os
import json
import inspect
import asyncio
from datetime import datetime, timedelta
from io import BytesIO
import logging
from concurrent.futures import ThreadPoolExecutor

import aiohttp
from PIL import Image

from api_engine import ApiEngine


io_pool_exc = ThreadPoolExecutor()


class SpaceCar:
    """
    This processing static class implements all the methods needed to get info about available scenes, get map grids
    with ground and car visualized images, to create merged images with ground satellite image as a background and also
    to save these images to the drive.
    Prepare all the API instances needed to get requested scenes and images
    """
    ragnar_search_api = ApiEngine('https://spaceknow-imagery.appspot.com/imagery/search')
    kraken_imagery_api = ApiEngine('https://spaceknow-kraken.appspot.com/kraken/release/imagery/geojson')
    kraken_cars_api = ApiEngine('https://spaceknow-kraken.appspot.com/kraken/release/cars/geojson')

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
        :param file_type: type of the supported file: truecolor.png/cars.png/detections.geojson...
        :return: bytes file
        """
        async with session.get(
                f'https://spaceknow-kraken.appspot.com/kraken/grid/{map_id}/-/'
                f'{tile_coordinates[0]}/{tile_coordinates[1]}/{tile_coordinates[2]}/{file_type}'
        ) as resp:
            if resp.status != 200:
                response = await resp.json()
                raise Exception(response.get('error', None), response.get('errorMessage', None))
            return await resp.read()

    @classmethod
    async def get_all_scenes(cls, geojson, days_ago=90):
        """
        Get all available scenes not older then days_ago parameter, for the extent specified by geojson
        :param geojson: Map extent specified in geojson format
        :param days_ago: not older than
        :return: all obtained scenes
        """
        scenes = await cls.ragnar_search_api.get_data(cls._get_search_payload(geojson, days_ago))
        logging.info("Scenes from search api were obtained")
        return scenes['results']

    @staticmethod
    def choose_best_scene(scenes):
        """
        Choose one from the obtained scenes. Decide based on visibility of the scene and chose the scene with the
        highest resolution.
        :param scenes: List of scenes
        :return: the chosen scene
        """
        best_scene = None
        for scene in scenes:
            if scene.get('cloudCover', 1) <= 0.30:
                if not best_scene:
                    best_scene = scene
                elif scene['bands'][0]['gsd'] < best_scene['bands'][0]['gsd']:
                    best_scene = scene

        best_scene = best_scene if best_scene else scenes['results'][0]
        logging.info(f"Scene {best_scene['sceneId']} was chosen")
        return best_scene

    @classmethod
    async def get_scene_maps(cls, geojson, scene):
        """
        Asynchronously get both, imagery and cars grid maps for the provided scene
        :param geojson: Map extent specified in geojson format
        :param scene:
        :return: maps
        """
        tasks = [
            cls.kraken_imagery_api.get_data(cls._get_release_payload(geojson, scene['sceneId'])),
            cls.kraken_cars_api.get_data(cls._get_release_payload(geojson, scene['sceneId']))
        ]

        imagery_map, cars_map = await asyncio.gather(*tasks)
        logging.info("Image maps for imagery and cars were obtained")

        return imagery_map, cars_map

    @classmethod
    async def get_scene_images(cls, imagery_map, cars_map):
        """
        Get all images from both maps
        :param imagery_map:
        :param cars_map:
        :return: list of image components - background, foreground image, detections info, image identification
        """
        async with aiohttp.ClientSession() as session:
            image_components = []
            for tile in imagery_map['tiles']:
                background = await cls._get_file(session, imagery_map['mapId'], tile)
                foreground = await cls._get_file(session, cars_map['mapId'], tile, file_type='cars.png')
                detections_info = await cls._get_file(session, cars_map['mapId'], tile, file_type='detections.geojson')
                image_name = f'{tile[1]}-{tile[2]}'
                image_components.append((background, foreground, detections_info, image_name))

            logging.info("Images were downloaded")
            return image_components

    @staticmethod
    def save_scene_images(image_components, scene):
        """
        Merge all background images with their respective foreground image and save them on the drive
        :param image_components: image component from get_map_image
        :param scene: scene used to identify output folder
        :return:
        """
        output_folder = f'{os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))}/../output'
        scene_name = scene['datetime'].replace(' ', '_')
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        if not os.path.exists(f'{output_folder}/{scene_name}'):
            os.makedirs(f'{output_folder}/{scene_name}')

        for background, foreground, detections_info, image_name in image_components:
            background_image = Image.open(BytesIO(background))
            foreground_image = Image.open(BytesIO(foreground))
            background_image.paste(foreground_image, (0, 0), foreground_image)
            background_image.save(f"{output_folder}/{scene_name}/{image_name}.png", "PNG")
            file = open(f"{output_folder}/{scene_name}/{image_name}.json", 'w')
            cars_count = len(json.loads(detections_info)['features'])
            json.dump(dict(cars_count=cars_count), file)
            file.close()
        logging.info(f"Images successfully saved to ./output/{scene_name}")

    @classmethod
    async def process_scene(cls, geojson, scene):
        """
        Process one scene - get maps, get images, save them
        :param geojson: Map extent specified in geojson format
        :param scene:
        :return:
        """
        logging.info(f"Processing scene with id {scene['sceneId']}")
        imagery_map, cars_map = await cls.get_scene_maps(geojson, scene)
        image_components = await cls.get_scene_images(imagery_map, cars_map)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(io_pool_exc, cls.save_scene_images, image_components, scene)

    @classmethod
    async def process_scenes(cls, geojson, scenes):
        """Process multiple scenes"""
        tasks = [cls.process_scene(geojson, scene) for scene in scenes]
        await asyncio.gather(*tasks)
