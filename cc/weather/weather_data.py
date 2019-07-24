#!/usr/bin/env python3

""" Weather Data class.
    - Caches weather data from Arable units in datastore.
    - Saves weather data from Arable units in bigquery.
"""

from datetime import datetime as dt, timedelta
import os, json, logging

from typing import Dict, List

from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import datastore
from cloud_common.cc.google import bigquery

class WeatherData:

    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        self.__name = os.path.basename(__file__)
        self.__kind = env_vars.ds_weather_entity
        if None == self.__kind:
            logging.critical(f'{self.__name} missing required environment '
                    f'variables.')


    #--------------------------------------------------------------------------
    # Return a list of arable devices, saved by the weather service into the
    # datastore.
    def get_arable_devices(self) -> List[str]:
        return datastore.get_keys(self.__kind)


    #--------------------------------------------------------------------------
    # Return a list of dicts of the computed weather data in the date range.  
    #
    # Input date strings must be in the format 'YYYY-MM-DD' and are inclusive.
    # arable_device_name is the arable device name.
    #
    # Returned data timestamps will be between every five minutes and 
    # hours / days.  So make no assumptions on intervals, the arable devices
    # are on cellular data and solar powered, so no guaranteed data.
    # 
    # Duplicate timestamp data has been filtered out.
    # The returned data is in DESCENDING order.
    #
    def get_computed_weather_data(self, 
            start_date: str, end_date: str, 
            arable_device_name: str) -> List[Dict]:
        # The datastore should have a cache of about 2500 of the last data
        # points from each device (assuming it has been collected by our
        # weather service and the device works).
        entity = datastore.get_by_key_from_DS(self.__kind, 
                arable_device_name) # Weather entity is keyed by device name.
        all_data_list = entity['computed'] # get the computed property
        # Create a list of dates in the range requested
        start = dt.strptime(start_date, '%Y-%m-%d')
        end = dt.strptime(end_date, '%Y-%m-%d')
        step = timedelta(days=1)
        dates = []
        while start <= end:
            dates.append(str(start.date()))
            start += step
        # Filter the data for the date range requested
        computed_data_list = []
        for json_str in all_data_list:
            d = json.loads(json_str)
            ts = d['time']
            date = ts[0:10] # first 10 chars of the timestamp is date
            if date in dates:
                computed_data_list.append(d)
        # Remove duplicate timestamps
        previous_ts = None
        for d in computed_data_list:
            if d['time'] == previous_ts:
                computed_data_list.remove(d)
                continue
            previous_ts = d['time'] 
        return computed_data_list # return the data 


#debugrob: add data writing methods from the weather_service.data_storage class
