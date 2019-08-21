#!/usr/bin/env python3

""" Notification Data class.
    - Stores and manages notifications in datastore.
    Design: https://github.com/OpenAgricultureFoundation/notification-service/blob/master/docs/API.pdf
"""

import datetime as dt
import json, logging, pprint

from typing import Dict, List

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import datastore

class NotificationData:

    # Keys used in DeviceData.notifications dict we store.
    ID_key           = "ID" # unique 6 digit random number
    type_key         = "type"
    message_key      = "message"
    created_key      = "created"
    URL_key          = "URL"

    # DeviceData property
    dd_property = datastore.DS_notifications_KEY

    # Notification types for display to user
    type_Done: str = "Done"   # show a Done button for user to click
    # other types later, such as Yes/No, Error, Warning, etc

    # For logging
    name: str = 'cloud_common.cc.notifications.notification_data'


    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        pass


    #--------------------------------------------------------------------------
    # private internal method
    # Get the list of all notifications for this device ID.
    def __get_all(self, device_ID: str) -> List[Dict[str, str]]:
        return datastore.get_device_data(self.dd_property, device_ID)


    #--------------------------------------------------------------------------
    # Return a string of the notifications for a device.  
    # For testing and debugging.
    def to_str(self, device_ID: str) -> str:
        pp = pprint.PrettyPrinter()
        out = pp.pformat(self.__get_all(device_ID))
        return out


    #--------------------------------------------------------------------------
    # Add a new notification for this device, set created TS to now().
    # Returns the notification ID string.
    def add(self, device_ID: str, message: str, 
            notification_type: str = type_Done,
            URL: str = None) -> str:
        notification_ID = utils.id_generator()
        now = dt.datetime.utcnow().strftime('%FT%XZ')

        # create a new dict
        notif_dict = {}
        notif_dict[self.ID_key] = notification_ID
        notif_dict[self.type_key] = notification_type
        notif_dict[self.message_key] = message
        notif_dict[self.created_key] = now
        notif_dict[self.URL_key] = URL

        # save the dict to the datastore
        datastore.save_device_data(device_ID, self.dd_property, notif_dict)

        return notification_ID


    #--------------------------------------------------------------------------
    # Returns a list of unacknowledged notifications dicts.
    def get_unacknowledged(self, device_ID: str) -> List[Dict[str, str]]:
        # every notification that we have is by definition 'un acknowledged'.
        return self.__get_all(device_ID)


    #--------------------------------------------------------------------------
    # Find notification by ID and delete it, to ack it.
    def ack(self, device_ID: str, notification_ID: str) -> None:
        entities = datastore.get_sharded_entities(
                datastore.DS_device_data_KIND, 
                self.dd_property, device_ID)
        for e in entities:
            data = e.get(datastore.DS_DeviceData_data_Property, {})
            if data.get(self.ID_key) == notification_ID:
                # delete this entity (as a form of acknowledging it and 
                # keeping the list of notifications from growing without 
                # bounds).
                DS = datastore.get_client()
                DS.delete(key=e.key)
                break


