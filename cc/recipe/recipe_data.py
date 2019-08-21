#!/usr/bin/env python3

""" Recipe Data class.
    - Stores the current application config in datastore and saves timestamped
      changes to bigquery for historical tracking.
    - Stores recipes in bigquery for historical tracking.
"""

from datetime import datetime as dt, timedelta
import os, json, logging, uuid, pprint, io

from typing import List, Dict

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
        config['timestamp'] = now
        ret = self.__save_DS(config)
        print(f'debugrob: write_config {config}')
        bigquery.save('recipe_generator_config', 
                config['devices_to_control'][0], # use first device in list
                now, config)
        return ret


    #--------------------------------------------------------------------------
    # Save a recipe to BQ (only, not to DS since nothing reads it).
    # This is true only for the recipe generator for the LGHC.
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
    # Private cache to datastore.
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



    #--------------------------------------------------------------------------
    # Create and return a recipe.
    def create_recipe(self, 
            recipe_name: str, 
            weather_data: List[Dict],
            times_to_repeat_last_day_in_recipe: int,
            days_length_in_recipe: int,
            light_distance_cm: int,
            compress_time: bool) -> str:

        if 0 >= len(weather_data):
            logging.error(f'{self.__name} create_recipe: no weather data.')

        if compress_time: 
            logging.warning(f'{self.__name} create_recipe: COMPRESSING time '
                    'in generated recipe.  The data recorded over an hour '
                    'will run in one minute in real time')

        template_recipe_dict = {
            "format": "openag-phased-environment-v1",
            "version": "4.0.1",
            "creation_timestamp_utc": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "name": recipe_name,
            "uuid": str(uuid.uuid4()),
            "parent_recipe_uuid": None,
            "support_recipe_uuids": None,
            "description": {
                "brief": "Created by recipe generator service",
                "verbose": "Created by recipe generator service",
            },
            "authors": [],
            "cultivars": [],
            "cultivation_methods": [],
            "environments": {},
            "phases": [],
        }

        # Use this for now, until we calibrate the LGHC COB and make an
        # LED peripheral setup with the spectrum mappings for it.
        PFC_sun_spectrum = {
            "380-399": 2.03, 
            "400-499": 20.3,
            "500-599": 23.27, 
            "600-700": 31.09, 
            "701-780": 23.31
        }

        # Iterate the weather data:
        logging.info(f'YYYY-MM-DD_HH:MM Temp   RH     PAR ')
        last_date = ''
        last_ts = None
        phase = {}
        # Iterate in date order (earliest to latest), this required looping
        # through the data array in reverse order.
        for i in range(len(weather_data) - 1, -1, -1): # start, end, step
            w = weather_data[i]
            ts = dt.strptime(w['time'], '%Y-%m-%dT%H:%M:%SZ')

            # If this is the first row / time, then just save the ts.
            if last_ts is None:
                last_ts = ts
                continue 

            # Name we will use for the enviroment and cycle
            date = f'{str(ts.date())}'
            name = f'{date}_{ts.hour:02}:{ts.minute:02}'

            temp = w['air_temp_degrees_C']
            RH = w['air_RH_percent']
            PAR = w['light_PAR_uE_m2_s']
            logging.info(f'{name} {ts.minute:02} '
                    f'{temp:4.2f} {RH:6.2f} {PAR:7.2f} ')

            # Add a named environment 
            template_recipe_dict["environments"][name] = {
                "name": name,
                "light_spectrum_nm_percent": PFC_sun_spectrum,
                "light_ppfd_umol_m2_s": PAR, 
                "light_illumination_distance_cm": light_distance_cm, 
                "air_temperature_celsius": temp,
                "air_humidity_percent": RH
            }

            # Add a new phase for every new calendar day.
            if last_date < date:
                last_date = date
                phase = {
                    "name": date,
                    "repeat": 1,   # this phase is one day long
                    "cycles": []   # filled in by next block of code
                }
                template_recipe_dict["phases"].append(phase)

            # Calculate how long it has been since the last interval to 
            # determine how long to run the cycle.  Float value can be 
            # < 1.0 for a partial hour.
            ts_delta = ts - last_ts
            last_ts = ts
            duration_hours = ts_delta.total_seconds() / 3600 # secs -> hours
            if 0 == ts_delta.total_seconds():
                continue # ignore duplicate HH:MM values

            # When TESTING, Rob is impatient, so we can compress time by 
            # making every hour equivalent to one minute.
            # (the smallest cycle the brain can handle is one minute)
            if compress_time: 
                duration_hours /= 60 
                if duration_hours < (1/60): # no less than 1/60 of an hour
                    duration_hours = (1/60) # one minute

            # Add one cycle to the daily phase for each time interval 
            # in that day.
            phase["cycles"].append({
                "name": name,        # just for human display
                "environment": name, # match the environment name (from above)
                "duration_hours": duration_hours
            })
            # end of looping over dates.

        # Repeat the last phase N times. 
        # (phase == day, repeat for insurance in case system has issues
        # getting more data)
        phase["repeat"] = times_to_repeat_last_day_in_recipe

        '''
        # For debugging, comment out for production
        recipe_file = 'recipe.raw' # to make sure date ordering is correct
        with open(recipe_file, 'w') as f:
            f.write(json.dumps(template_recipe_dict))
        recipe_file = 'recipe.json'
        with open(recipe_file, 'w') as f: # print dict to file
            pp = pprint.PrettyPrinter(stream=f) # pretty print SORTs keys!
            pp.pprint(template_recipe_dict)
        stream = io.StringIO(open(recipe_file).read().replace('\'', '"'))
        with open(recipe_file, 'w') as f:   # make it real JSON ' -> "
            f.write(stream.getvalue())
        logging.info(f'{self.__name} wrote {recipe_file}')
        '''

        # return the JSON recipe 
        return json.dumps(template_recipe_dict)



