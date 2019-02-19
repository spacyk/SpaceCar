import os
import asyncio
import logging

import aiohttp

JWT_TOKEN = os.environ.get("JWT_TOKEN", "")


class AuthenticationError(Exception):
    """When invalid token is used"""
    pass


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
                        raise Exception(error, response.get('errorMessage', None))
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
