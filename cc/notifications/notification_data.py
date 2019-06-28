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
    acknowledged_key = "acknowledged"
    URL_key          = "URL"

    # DeviceData property
    dd_property = "notifications"

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
        return datastore.get_device_data_property(device_ID, 
                self.dd_property)


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
        notif_dict[self.acknowledged_key] = None
        notif_dict[self.URL_key] = URL

        # get the existing list
        notif_list = self.__get_all(device_ID)
        if 1 == len(notif_list) and 0 == len(notif_list[0]):
            notif_list[0] = notif_dict # overwrite empty first dict
        else:
            notif_list.append(notif_dict) # append the new dict

        # save the list to the datastore
        datastore.save_list_as_device_data_queue(device_ID, 
                self.dd_property, notif_list)

        return notification_ID


    #--------------------------------------------------------------------------
    # Returns a list of unacknowledged notifications dicts.
    def get_unacknowledged(self, device_ID: str) -> List[Dict[str, str]]:
        unack_list = []
        notif_list = self.__get_all(device_ID)
        for n in notif_list:
            if n.get(self.acknowledged_key) is None: # not ackd
                unack_list.append(n)
        return unack_list 


    #--------------------------------------------------------------------------
    # Find notification by ID and update the acknowledged timestamp to now().
    def ack(self, device_ID: str, notification_ID: str) -> None:
        notif_list = self.__get_all(device_ID)
        for n in notif_list:
            if n.get(self.ID_key) == notification_ID:
                # update ack to now
                now = dt.datetime.utcnow().strftime('%FT%XZ')
                n[self.acknowledged_key] = now
                # save list back to DS
                datastore.save_list_as_device_data_queue(device_ID, 
                    self.dd_property, notif_list)
                break



