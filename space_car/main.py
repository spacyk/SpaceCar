import asyncio
import argparse
from json import loads
import logging

import aiofiles

from space_car import SpaceCar

logging.basicConfig(level=logging.INFO)


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


async def main():
    file_path = get_file_path()
    geojson = await get_geojson(file_path)

    car_visualizer = SpaceCar()
    scenes = await car_visualizer.get_all_scenes(geojson)
    best_scene = car_visualizer.choose_best_scene(scenes)

    await car_visualizer.process_scene(geojson, best_scene)

if __name__ == "__main__":
    asyncio.run(main())
