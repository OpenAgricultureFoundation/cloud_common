#!/usr/bin/env python3

""" MQTT Messaging class.
    - Handles messages published by our devices.
"""

import os, sys, logging, ast, time, tempfile
from datetime import datetime

from typing import Dict

from cloud_common.cc import utils 
from cloud_common.cc import images 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import pubsub # takes 15 secs to load...
from cloud_common.cc.google import storage 
from cloud_common.cc.google import datastore 
from cloud_common.cc.google import bigquery 
from cloud_common.cc.notifications.notification_messaging import NotificationMessaging
from cloud_common.cc.mqtt.deprecated_image_chunking import DeprecatedImageChunking

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
    name = 'cloud_common.cc.mqtt.mqtt_messaging'


    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        self.notification_messaging = NotificationMessaging()


    #--------------------------------------------------------------------------
    # Parse a pubsub message and take action.
    def parse(self, device_ID: str, message: Dict[str, str]) -> None:
        if not self.validate_message(message):
            logging.error(f'{self.name}.parse: invalid message={message}')
            return 

        if self.messageType_Image == self.get_message_type(message):
            #logging.warning(f'{self.name}.parse: ignoring old chunked images '
            #        'from old clients.')
            deprecated = DeprecatedImageChunking()
            deprecated.save_old_chunked_image(message, device_ID)
            return 

        # New way of handling (already) uploaded images.  
        if self.messageType_ImageUpload == self.get_message_type(message):
            self.save_uploaded_image(message, device_ID)
            return

        # Device sent a recipe event (start or stop) and we must 
        # republish a notification message to the notifications topic
        # using our NotificationMessaging class.
        if self.messageType_RecipeEvent == self.get_message_type(message):
            action = message.get(self.recipeAction_KEY)
            message_type = None
            if action == 'start':
                message_type = NotificationMessaging.recipe_start
            elif action == 'stop':
                message_type = NotificationMessaging.recipe_stop
            elif action == 'end':
                message_type = NotificationMessaging.recipe_end
            if message_type is None:
                logging.error(f'{self.name}.parse: invalid recipe event '
                        f'action={action}')
                return
            name = message.get(self.recipeName_KEY)
            self.notification_messaging.publish(device_ID, message_type, name)
            return

        # Save the most recent data as properties on the Device entity in the
        # datastore.
        self.save_data_to_Device(message, device_ID)

        # Also insert into BQ (Env vars and command replies)
        rowsList = []
        if self.makeBQRowList(message, device_ID, rowsList):
            bigquery.data_insert(rowsList)


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
            if self.messageType_EnvVar != self.get_message_type(pydict) and \
            self.messageType_CommandReply != self.get_message_type(pydict):
                return

            # each received EnvVar type message must have these fields
            if not utils.key_in_dict(pydict, self.var_KEY ) or \
                not utils.key_in_dict(pydict, self.values_KEY ):
                logging.error('save_data_to_Device: Missing key(s) in dict.')
                return
            varName = pydict[ self.var_KEY ]

            value = self.__string_to_value( pydict[ self.values_KEY ] )
            name = self.__string_to_name( pydict[ self.values_KEY ] )
            valueToSave = { 
                'timestamp': str( time.strftime( '%FT%XZ', time.gmtime())),
                'name': str( name ),
                'value': str( value ) }

            datastore.push_dict_onto_device_data_queue(deviceId,
                    varName, valueToSave)

        except Exception as e:
            logging.critical(f"Exception in save_data_to_Device(): {e}")


    #--------------------------------------------------------------------------
    # Private method to get the value from a string of data from the device
    # or DB.  Handles weird stuff like a string in a string.
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


    #--------------------------------------------------------------------------
    # New way of handling images.  
    # The image has already been uploaded to a GCP bucket via a public
    # firebase cloud function.   (in an open and un-secured manner) 
    # This is just a message telling us it was done (over the secure IoT 
    # messaging) and gives us a hook to move the image and save its URL.
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

            start = datetime.now()
            # get a timedelta of the difference
            delta = datetime.now() - start

            # keep checking for image curl upload for 5 minutes
            while delta.total_seconds() <= 5 * 60:

                # Has this image already been handled?
                # (this can happen since google pub-sub is "at least once" 
                # message delivery, the same message can get delivered again)
                if storage.isUploadedImageInBucket(file_name, 
                        env_vars.cs_bucket):
                    logging.info(f'save_uploaded_image: file {file_name} '
                        f'already handled.')
                    break

                # Check if the file is in the upload bucket.
                if not storage.isUploadedImageInBucket(file_name, 
                        env_vars.cs_upload_bucket):
                    time.sleep(10)
                    delta = datetime.now() - start
                    logging.debug(f'save_uploaded_image: waited '
                            f'{delta.total_seconds()} secs for '
                            f'upload of {file_name}')
                    continue

                # Move image from one gstorage bucket to another:
                #   openag-public-image-uploads > openag-v1-images
                publicURL = storage.moveFileBetweenBuckets( 
                        env_vars.cs_upload_bucket, 
                        env_vars.cs_bucket, file_name)
                if publicURL is None:
                    logging.warning(f'save_uploaded_image: '
                        f'image already moved: {file_name}')
                    break

                #img_in_bucket = storage.isUploadedImageInBucket(file_name, 
                #        env_vars.cs_bucket)
                #logging.info(f'save_uploaded_image: {img_in_bucket} '
                #        f'file {file_name} is in {env_vars.cs_bucket}')

                # use named temporary files to download and resize the image
                f_split = os.path.splitext(file_name)
                with tempfile.NamedTemporaryFile(suffix=f_split[1]) \
                        as downloaded_image_fp:

                    downloaded = storage.downloadFile(downloaded_image_fp, 
                            env_vars.cs_bucket, file_name)
                    if not downloaded:
                        logging.error(f'save_uploaded_image: '
                                f'image not downloaded: {file_name}')

                    # save a medium sized version of the image
                    downloaded_image_fp.seek(0) # rewind to start of stream
                    with tempfile.NamedTemporaryFile(suffix=f_split[1]) \
                            as smaller_image_fp:

                        # halves each dimension by default
                        images.resize(downloaded_image_fp.name, 
                                smaller_image_fp.name)

                        fn = f_split[0] + '_medium' + f_split[1]
                        storage.uploadFile(smaller_image_fp, 
                                env_vars.cs_bucket, fn)

                    # save a small sized version of the image
                    downloaded_image_fp.seek(0) # rewind to start of stream
                    with tempfile.NamedTemporaryFile(suffix=f_split[1]) \
                            as smaller_image_fp:

                        images.resize(downloaded_image_fp.name, 
                                smaller_image_fp.name,
                                (640, 480)) # good for ani gif

                        fn = f_split[0] + '_small' + f_split[1]
                        storage.uploadFile(smaller_image_fp, 
                                env_vars.cs_bucket, fn)

                    # save a small sized version of the image
                    downloaded_image_fp.seek(0) # rewind to start of stream
                    with tempfile.NamedTemporaryFile(suffix=f_split[1]) \
                            as smaller_image_fp:

                        images.resize(downloaded_image_fp.name, 
                                smaller_image_fp.name,
                                (128, 128)) # thumbnail size

                        fn = f_split[0] + '_thumbnail' + f_split[1]
                        storage.uploadFile(smaller_image_fp, 
                                env_vars.cs_bucket, fn)

                # Put the URL in the datastore for the UI to use.
                datastore.saveImageURL(deviceId, publicURL, var_name)

                # Put the URL as an env. var in BQ.
                message_obj = {}
                # keep old message type, UI code may depend on it
                message_obj[ self.messageType_KEY ] = self.messageType_Image
                message_obj[ self.var_KEY ] = var_name
                valuesJson = "{'values':["
                valuesJson += "{'name':'URL', 'type':'str', 'value':'%s'}" % \
                    (publicURL)
                valuesJson += "]}"
                message_obj[ self.values_KEY ] = valuesJson

                # Generate the data that will be sent to BigQuery for insertion.
                # Each value must be a row that matches the table schema.
                rowsList = []
                if self.makeBQRowList(message_obj, deviceId, rowsList):
                    bigquery.data_insert(rowsList)

                delta = datetime.now() - start
                logging.info(f"save_uploaded_image: Done with {file_name} "
                        f"in {delta.total_seconds()} secs")
                break
    
            # Remove any files in the uploads bucket that are over 2 hours old
            storage.delete_files_over_two_hours_old(env_vars.cs_upload_bucket)

        except Exception as e:
            logging.critical(f"Exception in save_uploaded_image(): {e}")


