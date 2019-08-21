#!/usr/bin/env python3

""" Runs class.
    - Maintains a list of recipe runs in datastore.
    Design: https://github.com/OpenAgricultureFoundation/notification-service/blob/master/docs/API.pdf
"""

import datetime as dt
import json, logging, pprint

from typing import Dict, List

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import datastore


"""
Store runs in datastore.DeviceData_runs_<device_ID> as a list of dicts:
queue of the most recent 100 runs per device
{
    start: str <timestamp in UTC>,
    end: str <timestamp in UTC>,
    recipe_name: str <name of recipe>
}
"""

class Runs:

    # Dict keys
    start_key  = 'start'
    end_key    = 'end'
    recipe_key = 'recipe_name'

    # DeviceData property
    runs_property = datastore.DS_runs_KEY

    # For logging
    name: str = 'cloud_common.cc.notifications.runs'


    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        pass


    #--------------------------------------------------------------------------
    # Return the runs for a device.  For testing and debugging.
    def to_str(self, device_ID: str) -> str:
        pp = pprint.PrettyPrinter()
        out = pp.pformat(self.get_all(device_ID))
        return out


    #--------------------------------------------------------------------------
    # Get all the runs for this device.  A 'run' is a start/stop (or end) pair
    # of timestamps of a recipe that was run on the device.
    # Returns a list of dicts of the runs for this device as: 
    #   { start: str, end: str, recipe_name: str }
    #     start may be None if a recipe has never been run.
    #     end may be None if the run is in progress.
    def get_all(self, device_ID: str) -> List[Dict[str, str]]:
        return datastore.get_device_data(self.runs_property, device_ID)


    #--------------------------------------------------------------------------
    # Get the latest run for this device.
    # Returns a dict of:
    #   { start: str, end: str, recipe_name: str }
    #     start may be None if a recipe has never been run.
    #     end may be None if the run is in progress.
    def get_latest(self, device_ID: str) -> Dict[ str, str ]:
        runs = datastore.get_device_data(self.runs_property, device_ID, count=1)
        run = {}
        if 0 < len(runs):
            run = runs[0]
        return run


    #--------------------------------------------------------------------------
    # Start a new run for this device.
    #   { start: now(), end: None, recipe_name: recipe_name }
    def start(self, device_ID: str, recipe_name: str) -> None:
        run = {self.start_key:  dt.datetime.utcnow().strftime('%FT%XZ'),
               self.end_key:    None,
               self.recipe_key: recipe_name
        }
        datastore.save_device_data(device_ID, self.runs_property, run)


    #--------------------------------------------------------------------------
    # Stop an existing run for this device, if the top item on the queue 
    # has end == None, end is set to now().
    #   { start: TS, end: now() }
    def stop(self, device_ID: str) -> None:
        # have to get the datastore entity to update it.
        entities = datastore.get_sharded_entities(
                datastore.DS_device_data_KIND, 
                self.runs_property, device_ID, count=1)
        if 0 == len(entities):
            logging.error(f'{self.name}.stop no current run for {device_ID}')
            return

        e = entities[0] # only one entity in the list
        # get this entities data property and update it
        run = e.get(datastore.DS_DeviceData_data_Property, {})
        run[self.end_key] = dt.datetime.utcnow().strftime('%FT%XZ')

        # put entity back in datastore
        DS = datastore.get_client()
        DS.put(e)
        logging.debug(f'{self.name}.stopped run {run}')




