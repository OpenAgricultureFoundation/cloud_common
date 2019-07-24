#!/usr/bin/env python3

""" Recipe Data class.
    - Stores the current application config in datastore and saves timestamped
      changes to bigquery for historical tracking.
    - Stores recipes in bigquery for historical tracking.
"""

from datetime import datetime as dt
import os, json, logging

from typing import List

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import datastore
from cloud_common.cc.google import bigquery

class RecipeData:

    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        self.__name = os.path.basename(__file__)
        self.__kind = env_vars.ds_recipe_entity
        self.__key = env_vars.ds_recipe_config
        if None == self.__kind or None == self.__key:
            logging.critical(f'{self.__name} missing required environment '
                    f'variables.')


    #--------------------------------------------------------------------------
    # Read the application configuration info from datastore.
    # Returns a dictionary of the config info, or None.
    def read_config(self) -> dict:
        entity = datastore.get_by_key_from_DS(self.__kind, self.__key)
        json_config = entity[self.__key]
        config_dict = json.loads(utils.bytes_to_string(json_config))
        return config_dict


    #--------------------------------------------------------------------------
    # Write the application configuration info to datastore and to bigquery
    # (for historical change tracking). 
    def write_config(self, config: dict) -> None:
        now = dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        bigquery.save('recipe_generator_config', 
                config['devices_to_control'][0], # use first device in list
                now, config)
        config['timestamp'] = now
        return self.__save_DS(config)


    #--------------------------------------------------------------------------
    # Save a recipe to BQ (only, not to DS since nothing reads it).
    # Returns True for success, False for error.
    def save_recipe(self, 
            arable_device_name: str, 
            timestamp: str, 
            data: dict) -> bool:
        try:
            if timestamp is None or arable_device_name is None or \
                    0 == len(data):
                logging.error(f'{self.__name} save_recipe: invalid args')
                return False

            if not bigquery.save('recipe', arable_device_name, 
                    timestamp, data):
                logging.error(f'{self.__name} save_recipe: BQ save failed.')
                return False

            return True
        except Exception as e:
            logging.error(f'save_recipe: {e}')
            return False


    #--------------------------------------------------------------------------
    # Private save to datastore.
    # Returns True for success, False for error.
    def __save_DS(self, data: dict) -> bool:
        try:
            if data is None:
                logging.error(f'{self.__name} __save_DS: invalid args.')
                return False
            json_data = json.dumps(data)
            return datastore.save_with_key(self.__kind, self.__key, json_data)
        except Exception as e:
            logging.error(f'{self.__name} __save_DS: {e}')
            return False



