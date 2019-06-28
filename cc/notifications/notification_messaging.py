#!/usr/bin/env python3

""" Notification Messaging class.
    
    - Handles recipe start/stop/end messages.
    - Uses the Scheduler class.
    - Uses the Runs class.

    Design: https://github.com/OpenAgricultureFoundation/notification-service/blob/master/docs/API.pdf
"""

import os, sys, json, argparse, logging, signal

from typing import Dict

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import pubsub # takes 15 secs to load...
from cloud_common.cc.notifications.scheduler import Scheduler
from cloud_common.cc.notifications.runs import Runs

class NotificationMessaging:

    # Message types we send and receive
    recipe_start      = 'recipe_start'      # new recipe started
    recipe_stop       = 'recipe_stop'       # user stopped it
    recipe_end        = 'recipe_end'        # recipe concluded naturally
    set_testing_hours = 'set_testing_hours' # just for testing schedule

    # Message dict keys
    device_ID_key    = "device_ID"
    message_type_key = "message_type"
    message_key      = "message"  # recipe_name for the recipe_* messages

    # For logging
    name = 'cloud_common.cc.notifications.notification_messaging'


    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        pass # nothing to do to construct this class


    #--------------------------------------------------------------------------
    # Publish a message to the notifications topic (defined in env. var).
    def publish(self, device_ID: str, message_type: str, message: str = '') -> None:

        if len(device_ID) == 0:
            logging.error(f'{self.name}.publish: invalid device_ID.')
            return

        if message_type is not self.recipe_start and \
           message_type is not self.recipe_stop and \
           message_type is not self.recipe_end and \
           message_type is not self.set_testing_hours:
            logging.error(f'{self.name}.publish: '
                    f'invalid message_type={message_type}')
            return

        msg_dict = {
            self.device_ID_key: device_ID,
            self.message_type_key: message_type,
            self.message_key: message,
        }
        pubsub.publish(env_vars.cloud_project_id, 
            env_vars.notifications_topic_subs, msg_dict)


    #--------------------------------------------------------------------------
    # Validate the pubsub message we received.
    # Returns True for valid, False otherwise.
    def validate_message(self, message: Dict[str, str]) -> bool:
        if not utils.key_in_dict(message, self.device_ID_key):
            return False
        if not utils.key_in_dict(message, self.message_type_key):
            return False
        if not utils.key_in_dict(message, self.message_key):
            return False
        message_type = message.get(self.message_type_key)
        if not (message_type == self.recipe_start or \
                message_type == self.recipe_stop or \
                message_type == self.recipe_end or \
                message_type == self.set_testing_hours):
            return False
        return True


    #--------------------------------------------------------------------------
    # Parse a pubsub message and take action.
    def parse(self, message: Dict[str, str]) -> None:
        if not self.validate_message(message):
            logging.error(f'{self.name}.parse: invalid message={message}')
            return 

        s = Scheduler()
        r = Runs()

        device_ID    = message.get(self.device_ID_key)
        message_type = message.get(self.message_type_key)
        recipe_name  = message.get(self.message_key)

        # Start a new recipe
        if message_type == self.recipe_start:
            # New task to check fluids using the default interval hours (48).
            s.add(device_ID, Scheduler.check_fluid_command)

            # New task to prune the plant every 48h (with video)
            s.add(device_ID, Scheduler.prune_plant_command)

            # New task to take plant measurements in 7 days (the first time),
            # then every default interval (48 hours) after that.
            s.add(device_ID, Scheduler.take_measurements_command, 24 * 7)

            # Start tracking this new run.  For UI charting.
            r.start(device_ID, recipe_name)

        # Stop a recipe
        elif message_type == self.recipe_stop:

            # Remove all commands.
            s.remove_all_commands(device_ID)

            # Mark the current run as stopped.
            r.stop(device_ID)

        # End a recipe
        elif message_type == self.recipe_end:
            # Remove all commands.
            s.remove_all_commands(device_ID)

            # Add a one-off notification right now (not scheduled).
            s.create_notification(device_ID, Scheduler.harvest_plant_command) 

            # Mark the current run as stopped.
            r.stop(device_ID)

        # Just for testing, by sending a message with the test offset hours.
        elif message_type == self.set_testing_hours:
            hours = int(recipe_name)
            s.set_testing_hours(hours)

        # For all messages received, check the schedule to see if anything
        # is due to be run.
        s.check(device_ID)



