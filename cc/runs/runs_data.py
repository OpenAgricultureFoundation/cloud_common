#!/usr/bin/env python3

""" Runs Data class.
    - Returns list of recipe runs: start/end times from datastore.
"""

from datetime import datetime as dt, timedelta
import os, json, logging

from typing import Dict, List

from cloud_common.cc.google import datastore

class RunsData:

    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        self.__name = os.path.basename(__file__)

    #--------------------------------------------------------------------------
    # Return a list of dicts that contain:
    #    {"recipe_name":"Demo Rainbow",
    #     "end":"2019-08-09T16:53:29Z",
    #     "start":"2019-08-09T16:52:46Z"}
    def get_runs(self, device_ID) -> List[Dict[str, str]]:
        return datastore.get_device_data(datastore.DS_runs_KEY, device_ID)


