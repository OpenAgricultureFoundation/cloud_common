#!/usr/bin/env python3

""" MQTT Messaging class.
    - Handles messages published by our devices.
"""

import os, sys, logging, ast, time, tempfile
import re
from datetime import datetime

from typing import Dict

from cloud_common.cc import utils 
from cloud_common.cc import images

# No need for any of the google stuff since we're trying to run local
#from cloud_common.cc.google import env_vars
#from cloud_common.cc.google import pubsub # takes 15 secs to load...
#from cloud_common.cc.google import storage
#from cloud_common.cc.google import datastore
#from cloud_common.cc.google import bigquery

from influxdb import InfluxDBClient

# Temporarily disable notifications
#from cloud_common.cc.notifications.notification_messaging import NotificationMessaging
#from cloud_common.cc.notifications.runs import Runs

# No longer need the image chunking since we won't be using it locally
#from cloud_common.cc.mqtt.deprecated_image_chunking import DeprecatedImageChunking

class MQTTMessaging:

    # keys common to all messages
    messageType_KEY = 'messageType'
    messageType_EnvVar = 'EnvVar'
    messageType_CommandReply = 'CommandReply'
    # Deprecate processing of chunked 'Image' messages (from old brains), 
    # but keep the type for UI backwards compatability.
    messageType_Image = 'Image' 
    messageType_ImageUpload = 'ImageUpload'
    messageType_RecipeEvent = 'RecipeEvent'

    # keys for messageType='EnvVar' (and also 'CommandReply')
    var_KEY = 'var'
    values_KEY = 'values'

    # keys for messageType='Image' (and uploads)
    varName_KEY = 'varName'
    imageType_KEY = 'imageType'
    fileName_KEY = 'fileName'

    # keys for messageType='RecipeEvent' 
    recipeAction_KEY = 'action'
    recipeName_KEY = 'name'

    # keys for datastore entities
    DS_device_data_KEY = 'DeviceData'
    DS_env_vars_MAX_size = 100 # maximum number of values in each env. var list
    DS_images_KEY = 'Images'

    # For logging
    name = 'cloud_common.cc.mqtt.local_mqtt_messaging'


    #--------------------------------------------------------------------------
    def __init__(self, host='influxdb', port=8086, db_name='openag_local') -> None:
        # self.notification_messaging = NotificationMessaging()
        # self.runs = Runs()
        self.influx = InfluxDBClient(host=host, port=port)
        self.influxdb_name = db_name
        self.influx.switch_database(self.influxdb_name)


    #--------------------------------------------------------------------------
    # Parse a pubsub message and take action.
    def parse(self, device_ID: str, message: Dict[str, str]) -> None:
        if not self.validate_message(message):
            logging.error(f'{self.name}.parse: invalid message={message}')
            return 

        # This is for old image chunking stuff, so remove it
        # if self.messageType_Image == self.get_message_type(message):
        #     #logging.warning(f'{self.name}.parse: ignoring old chunked images '
        #     #        'from old clients.')
        #     deprecated = DeprecatedImageChunking()
        #     deprecated.save_old_chunked_image(message, device_ID)
        #     return

        # New way of handling (already) uploaded images.  
        if self.messageType_ImageUpload == self.get_message_type(message):
            self.save_uploaded_image(message, device_ID)
            return

    # TODO: For local servers, figureout 'runs'
        # Device sent a recipe event (start or stop) and we must 
        # republish a notification message to the notifications topic
        # using our NotificationMessaging class.
    #    if self.messageType_RecipeEvent == self.get_message_type(message):
    #        action = message.get(self.recipeAction_KEY)
    #        message_type = None
    #        name = message.get(self.recipeName_KEY)
    #        if action == 'start':
    #            message_type = NotificationMessaging.recipe_start
    #            self.runs.start(device_ID,name)
    #        elif action == 'stop':
    #            message_type = NotificationMessaging.recipe_stop
    #            self.runs.stop(device_ID)
    #        elif action == 'end':
    #            message_type = NotificationMessaging.recipe_end
    #            self.runs.stop(device_ID)
    #        if message_type is None:
    #            logging.error(f'{self.name}.parse: invalid recipe event '
    #                    f'action={action}')
    #            return
    #        # TODO: Re-enable this when notifications get turned back on (After removing saving of the
    #        #   runs() data from the notification service.
    #        # self.notification_messaging.publish(device_ID, message_type, name)
    #        return

        # NEW LOCAL DATA --
        # First we will save all data to influx db

        self.save_data_to_Device(message, device_ID)


    #--------------------------------------------------------------------------
    # Validate the pubsub message we received.
    # Returns True for valid, False otherwise.
    def validate_message(self, message: Dict[str, str]) -> bool:
        if not utils.key_in_dict(message, self.messageType_KEY):
            return False
        message_type = self.get_message_type(message)
        if not (message_type == self.messageType_EnvVar or \
                message_type == self.messageType_CommandReply or \
                message_type == self.messageType_Image or \
                message_type == self.messageType_ImageUpload or \
                message_type == self.messageType_RecipeEvent):
            return False
        if message_type == self.messageType_EnvVar or \
                message_type == self.messageType_CommandReply:
            # mandatory keys for msg types 'EnvVar' and 'CommandReply'
            if not (utils.key_in_dict(message, self.var_KEY) or \
                    utils.key_in_dict(message, self.values_KEY)):
                return False
        if message_type == self.messageType_Image or \
                message_type == self.messageType_ImageUpload:
            # mandatory keys for image messages
            if not (utils.key_in_dict(message, self.varName_KEY) or \
                    utils.key_in_dict(message, self.imageType_KEY) or \
                    utils.key_in_dict(message, self.fileName_KEY)):
                return False
        if message_type == self.messageType_RecipeEvent:
            # mandatory keys for recipe event messages
            if not (utils.key_in_dict(message, self.recipeAction_KEY) or \
                    utils.key_in_dict(message, self.recipeName_KEY)):
                return False
        return True


    #--------------------------------------------------------------------------
    # Returns the messageType key if valid, else None.
    def get_message_type(self, message):
        if not utils.key_in_dict(message, self.messageType_KEY):
            logging.error('Missing key %s' % self.messageType_KEY)
            return None

        if self.messageType_EnvVar == message.get(self.messageType_KEY):
            return self.messageType_EnvVar

        if self.messageType_CommandReply == message.get(self.messageType_KEY):
            return self.messageType_CommandReply

        # deprecated
        if self.messageType_Image == message.get(self.messageType_KEY):
            return self.messageType_Image

        if self.messageType_ImageUpload == message.get(self.messageType_KEY):
            return self.messageType_ImageUpload

        if self.messageType_RecipeEvent == message.get(self.messageType_KEY):
            return self.messageType_RecipeEvent

        logging.error('get_message_type: Invalid value {} for key {}'.format(
            message.get(self.messageType_KEY), self.messageType_KEY ))
        return None


    #--------------------------------------------------------------------------
    # Make a BQ row that matches the table schema for the 'vals' table.
    # (python will pass only mutable objects (list) by reference)
    def makeBQEnvVarRowList(self, valueDict, deviceId, rowsList, idKey):
        # each received EnvVar type message must have these fields
        if not utils.key_in_dict(valueDict, self.var_KEY ) or \
           not utils.key_in_dict(valueDict, self.values_KEY ):
            logging.error('makeBQEnvVarRowList: Missing key(s) in dict.')
            return

        varName = valueDict[ self.var_KEY ]
        values = valueDict[ self.values_KEY ]

        # clean / scrub / check the values.  
        deviceId = deviceId.replace( '~', '' ) 
        varName = varName.replace( '~', '' ) 

        # NEW ID format:  <KEY>~<valName>~<created UTC TS>~<deviceId>
        ID = idKey + '~{}~{}~' + deviceId

        row = (ID.format(varName, 
            time.strftime('%FT%XZ', time.gmtime())), # id column
            values, 0, 0) # values column, with zero for X, Y

        rowsList.append(row)


    #--------------------------------------------------------------------------
    # returns True if there are rows to insert into BQ, false otherwise.
    def makeBQRowList(self, valueDict, deviceId, rowsList):

        messageType = self.get_message_type( valueDict )
        if None == messageType:
            return False

        # write envVars and images (as envVars)
        if self.messageType_EnvVar == messageType or \
           self.messageType_Image == messageType:
            self.makeBQEnvVarRowList( valueDict, deviceId, rowsList, 'Env' )
            return True

        if self.messageType_CommandReply == messageType:
            self.makeBQEnvVarRowList( valueDict, deviceId, rowsList, 'Cmd' )
            return True

        return False


    #--------------------------------------------------------------------------
    # Save a bounded list of the recent values of each env. var. to the Device
    # that produced them - for UI display / charting.
    def save_data_to_Device(self, pydict, deviceId):
        try:
            message_type = self.get_message_type(pydict)
            if self.messageType_EnvVar != message_type and \
            self.messageType_CommandReply != message_type:
                logging.debug(f"save_data_to_Device: invalid message type {message_type}")
                return

            # each received EnvVar type message must have these fields
            if not utils.key_in_dict(pydict, self.var_KEY ) or \
                not utils.key_in_dict(pydict, self.values_KEY ):
                logging.error('save_data_to_Device: Missing key(s) in dict.')
                return
            varName = pydict[ self.var_KEY ]


            name = self.__string_to_name( pydict[ self.values_KEY ] )
            #valueToSave = {
            #    'timestamp': str( time.strftime( '%FT%XZ', time.gmtime())),
            #    'name': str( name ),
            #    'value': str( value ) }
            # Influx values below
            # TODO: fix this hack to deal with spectral bands - replacing '###-###': with 'band_###-###':
            values_string = pydict[self.values_KEY]
            influx_tags = {"device_id": deviceId, "sensor": str(name), "isSpectrum": False}
            if varName == 'light_spectrum_nm_percent':
                values_string = re.sub("\\'(\d+-\d+)\\'","\"band_\\1\"", values_string)
                influx_tags["isSpectrum"] = True

            value = self.__string_to_value(values_string)
            # For influxDB figure out the 'measurement' to use.
            measurement_type = "env_vars"  # most likely this is what we'll be using
            if message_type == self.messageType_CommandReply:
                measurement_type = str(varName)  # 'status' or 'boot'

            valueToSave = {
                "measurement": measurement_type,
                "time": str( time.strftime( '%FT%XZ', time.gmtime())),
                "tags": influx_tags,
                "fields": {varName: value}
            }
            logging.debug(valueToSave)
            self.influx.write_points([valueToSave])
            #datastore.save_device_data(deviceId, varName, valueToSave)

        except Exception as e:
            logging.critical(f"Exception in save_data_to_Device(): {e}")


    #--------------------------------------------------------------------------
    # Private method to get the value from a string of data from the device
    # or DB.  Handles weird stuff like a string in a string.
    # TODO: This will only handle one value?
    def __string_to_value(self, string):
        try:
            values = ast.literal_eval( string ) # if this works, great!
            firstVal = values['values'][0]
            return firstVal['value']
        except:
            # If the above has issues, the string probably has an embedded string.
            # Such as this:
            # "{'values':[{'name':'LEDPanel-Top', 'type':'str', 'value':'{'400-449': 0.0, '450-499': 0.0, '500-549': 83.33, '550-559': 16.67, '600-649': 0.0, '650-699': 0.0}'}]}"
            valueTag = "\'value\':\'"
            endTag = "}]}"
            valueStart = string.find( valueTag )
            valueEnd = string.find( endTag )
            if -1 == valueStart or -1 == valueEnd:
                return string
            valueStart += len( valueTag )
            valueEnd -= 1
            val = string[ valueStart:valueEnd ]
            logging.error("parsed value : " + val)
            return ast.literal_eval(val) # let exceptions from this flow up
        return string


    #--------------------------------------------------------------------------
    # Private method to get the name from a string of data from the device
    # or DB.  Handles weird stuff like a string in a string.
    def __string_to_name(self, string):
        try:
            values = ast.literal_eval( string ) # if this works, great!
            firstVal = values['values'][0]
            return firstVal['name']
        except:
            # If the above has issues, the string probably has an embedded string.
            # Such as this:
            # "{'values':[{'name':'LEDPanel-Top', 'type':'str', 'value':'{'400-449': 0.0, '450-499': 0.0, '500-549': 83.33, '550-559': 16.67, '600-649': 0.0, '650-699': 0.0}'}]}"
            nameTag = "\'name\':\'"
            endTag = "\'"
            nameStart = string.find( nameTag )
            if -1 == nameStart:
                return None
            nameStart += len( nameTag )
            nameEnd = string.find( endTag, nameStart )
            if -1 == nameEnd:
                return None
            name = string[ nameStart:nameEnd ]
            return name
        return ''


    def save_uploaded_image(self, pydict, deviceId):
         try:
             if self.messageType_ImageUpload != self.get_message_type(pydict):
                 logging.error("save_uploaded_image: invalid message type")
                 return

             # each received image message must have these fields
             if not utils.key_in_dict(pydict, self.varName_KEY) or \
             not utils.key_in_dict(pydict, self.fileName_KEY ):
                 logging.error('save_uploaded_image: missing key(s) in dict.')
                 return

             var_name =  pydict.get(self.varName_KEY)
             file_name = pydict.get(self.fileName_KEY)

             # var_name is bad (problem in brain code), so we'll parse the fileName
             # EDU-6B1261EF-b8-27-eb-7f-f2-73_2020-06-05_T19-42-52Z_Camera-Top.png
             fileDeviceId, imageDate, imageTime, sensorName = file_name.split("_")
             sensorName = sensorName.split(".")[0]
             measurement_type = "images"
             influx_tags = {"device_id": deviceId, "sensor": str(sensorName)}

             valueToSave = {
                 "measurement": measurement_type,
                 "time": str("{}{}".format(imageDate, imageTime.replace("-",":"))),
                 "tags": influx_tags,
                 "fields": {"filename": file_name}
             }
             logging.debug(valueToSave)
             self.influx.write_points([valueToSave])

         except Exception as e:
            logging.critical(f"Exception in save_uploaded_image(): {e}")


