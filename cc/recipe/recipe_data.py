#!/usr/bin/env python3

""" Recipe Data class.
    - Stores the current application config in datastore and saves timestamped
      changes to bigquery for historical tracking.
    - Stores recipes in bigquery for historical tracking.
"""

from datetime import datetime as dt
import os, json, logging, uuid

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
        bigquery.save('recipe_generator_config', 
                config['devices_to_control'][0], # use first device in list
                now, config)
        config['timestamp'] = now
        return self.__save_DS(config)


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
            days_length_in_recipe: int) -> str:

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

#debugrob:  fill in environments and phases + cycles from weather data

#debugrob: cycles times are fractional hours!
#              "duration_hours": 0.001},  # will quickly flip to day/night/...

#debugrob: below is just a hack.
        template_recipe_dict["environments"] = {
    "standard_day": {
        "name": "Standard Day", 
        "light_spectrum_nm_percent": {
            "380-399": 2.03, 
            "400-499": 20.3,
            "500-599": 23.27, 
            "600-700": 31.09, 
            "701-780": 23.31}, 
        "light_ppfd_umol_m2_s": 300, 
        "light_illumination_distance_cm": 10, 
        "air_temperature_celsius": 22},
    "standard_night": {
        "name": "Standard Night", 
        "light_spectrum_nm_percent": {
            "380-399": 0.0, 
            "400-499": 0.0, 
            "500-599": 0.0,
            "600-700": 0.0, 
            "701-780": 0.0}, 
        "light_ppfd_umol_m2_s": 0,
        "light_illumination_distance_cm": 10, 
        "air_temperature_celsius": 22},
        }

        template_recipe_dict["phases"] = [
        {"name": "Standard Growth", 
         "repeat": 29, 
         "cycles": [
            {"name": "Day", 
             "environment": "standard_day", 
              "duration_hours": 0.001}, 
            {"name": "Night", 
             "environment": "standard_night", 
              "duration_hours": 0.001}
            ]
        }
        ]

        return json.dumps(template_recipe_dict)

#debugrob: string dumped from datastore Recipes, and it works.
#        return '{"format": "openag-phased-environment-v1", "version": "0.1.2", "creation_timestamp_utc": "2018-07-19T16:54:24:44Z", "name": "Rob is hacking", "uuid": "abdcebe7-d496-43cc-8bd3-3a40a79e854e", "parent_recipe_uuid": null, "support_recipe_uuids": null, "description": {"brief": "Grows basil.", "verbose": "Grows basil."}, "authors": [{"name": "OpenAgTest", "uuid": "1e91ef7d-e9c2-4b0d-8904-f262a9eda70d", "email": "rp492@cornell.edu"}], "cultivars": [{"name": "Basil/Sweet Basil", "uuid": "02b0328f-ff19-44a8-a8b8-cd13cf6b80af"}], "cultivation_methods": [{"name": "Shallow Water Culture", "uuid": "45fa509b-2008-4109-a39e-e5682c421925"}], "environments": { "standard_day": { "name": "Standard Day", "light_spectrum_nm_percent": { "380-399": 2.03, "400-499": 20.3, "500-599": 23.27, "600-700": 31.09, "701-780": 23.31}, "light_ppfd_umol_m2_s": 300, "light_illumination_distance_cm": 10, "air_temperature_celsius": 22}, "standard_night": { "name": "Standard Night", "light_spectrum_nm_percent": { "380-399": 0.0, "400-499": 0.0, "500-599": 0.0, "600-700": 0.0, "701-780": 0.0}, "light_ppfd_umol_m2_s": 0, "light_illumination_distance_cm": 10, "air_temperature_celsius": 22} }, "phases": [ {"name": "Standard Growth", "repeat": 29, "cycles": [ {"name": "Day", "environment": "standard_day", "duration_hours": 18}, {"name": "Night", "environment": "standard_night", "duration_hours": 6} ] } ] } '

'''
#debugrob: this is the RecipeSchema in DS, key by format string:
{
	"type": "object",
	"properties": {
		"format": {
			"type": "string",
			"enum": ["openag-phased-environment-v1"]
		},
		"version": {"type": "string"},
		"creation_timestamp_utc": {"type": "string"},
		"name": {"type": "string"},
		"uuid": {"type": "string"},
		"parent_recipe_uuid": {"type": ["string", "null"]},
		"support_recipe_uuids": {"type": "null"},
		"description": {
			"type": "object",
			"parameters": {
				"brief": {"type": "string"},
				"verbose": {"type": "string"}
			},
			"required": ["brief", "verbose"]
		},
		"authors": {
			"type": "array",
			"items": {
				"type": "object",
				"parameters": {
					"name": {"type": "string"},
					"email": {"type": ["string", "null"]},
					"uuid": {"type": "string"}
				},
				"required": ["name", "email", "uuid"]
			}
		},
		"cultivars": {
			"type": "array",
			"items": {
				"type": "object",
				"parameters": {
					"name": {"type": "string"},
					"uuid": {"type": "string"}
				},
				"required": ["name", "uuid"]
			}
		},
		"cultivation_methods": {
			"type": "array",
			"items": {
				"type": "object",
				"parameters": {
					"name": {"type": "string"},
					"uuid": {"type": "string"}
				},
				"required": ["name", "uuid"]
			}
		},
		"environments": {"type": "object"},
		"phases": {
			"type": "array",
			"items": {
				"type": "object",
				"parameters": {
					"name": {"type": "string"},
					"repeat": {"type": "string"},
					"cycles": {
						"type": "array",
						"parameters": {
							"name": {"type": "string"},
							"environment": {"type": "string"},
							"duration_hours": {"type": "integer"}
						},
						"required": ["name", "environment", "duration_hours"]
					}
				},
				"required": ["name", "repeat", "cycles"]
			}
		}
	},
	"required": [
		"format", 
		"version", 
		"creation_timestamp_utc",
		"name",
		"uuid",
		"parent_recipe_uuid",
		"support_recipe_uuids",
		"description",
		"authors",
		"cultivars",
		"cultivation_methods",
		"environments",
		"phases"
	]
}
    
#debugrob: this is our get growing v4 recipe in DS:
{"format": "openag-phased-environment-v1", 
"version": "0.1.2",
"creation_timestamp_utc": "2018-07-19T16:54:24:44Z", 
"name": "Get Growing - Basil Recipe", 
"uuid": "e6085be7-d496-43cc-8bd3-3a40a79e854e",
"parent_recipe_uuid": "37dc0177-076a-4903-8557-c7586e42e90e",
"support_recipe_uuids": null, 
"description": {"brief": "Grows basil.",
                "verbose": "Grows basil."}, 
"authors": [{"name": "OpenAgTest", 
             "uuid": "1e91ef7d-e9c2-4b0d-8904-f262a9eda70d", 
             "email": "rp492@cornell.edu"}],
"cultivars": [{"name": "Basil/Sweet Basil", 
               "uuid": "02b0328f-ff19-44a8-a8b8-cd13cf6b80af"}], 
"cultivation_methods": [{"name": "Shallow Water Culture", 
                         "uuid": "45fa509b-2008-4109-a39e-e5682c421925"}],
"environments": {
    "standard_day": {
        "name": "Standard Day", 
        "light_spectrum_nm_percent": {
            "380-399": 2.03, 
            "400-499": 20.3,
            "500-599": 23.27, 
            "600-700": 31.09, 
            "701-780": 23.31}, 
        "light_ppfd_umol_m2_s": 300, 
        "light_illumination_distance_cm": 10, 
        "air_temperature_celsius": 22},
    "standard_night": {
        "name": "Standard Night", 
        "light_spectrum_nm_percent": {
            "380-399": 0.0, 
            "400-499": 0.0, 
            "500-599": 0.0,
            "600-700": 0.0, 
            "701-780": 0.0}, 
        "light_ppfd_umol_m2_s": 0,
        "light_illumination_distance_cm": 10, 
        "air_temperature_celsius": 22},
},
"phases": [
        {"name": "Standard Growth", 
         "repeat": 29, 
         "cycles": [
            {"name": "Day", 
             "environment": "standard_day", 
              "duration_hours": 18}, 
            {"name": "Night", 
             "environment": "standard_night", 
              "duration_hours": 6}
            ]
        }
    ]
}
'''

