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
        return self.get_all(device_ID)[0] # return top of the list


    #--------------------------------------------------------------------------
    # Start a new run for this device.
    # Push onto the queue:
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
        run = self.get_latest(device_ID)
        if run is None or run == {}:
            logging.error(f'{self.name}.stop no current run for {device_ID}')
            return
        run[self.end_key] = dt.datetime.utcnow().strftime('%FT%XZ')

        # get the list of all runs for this device
        all_runs_list = self.get_all(device_ID)

        # overwrite the top of the list (latest) run
        all_runs_list[0] = run

        # put back in datastore
        datastore.save_device_data(device_ID, self.runs_property, all_runs_list)
        logging.debug(f'{self.name}.stop-ped run {run}')




