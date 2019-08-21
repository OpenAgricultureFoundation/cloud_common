#!/usr/bin/env python3

""" Scheduler class.
    - Maintains a schedule of repeating events in datastore.
    Design: https://github.com/OpenAgricultureFoundation/notification-service/blob/master/docs/API.pdf
"""

import datetime as dt
import json, logging, pprint

from typing import Dict, List, Any

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import datastore
from cloud_common.cc.notifications.notification_data import NotificationData


"""
Store schedule in datastore.DeviceData_schedule_<device_ID> as a dict:
{
    command: str <command>,
    message: str <message to display>,
    run_at:  str <timestamp to run on>,
    repeat:  int <number of hours, can be 0 for a one time command>,
    count:   int <execution count>
}
"""
class Scheduler:

    # Keys used in DeviceData.schedule dict we store.
    command_key = 'command'
    message_key = 'message'
    run_at_key  = 'run_at'
    repeat_key  = 'repeat'
    count_key   = 'count'
    URL_key     = 'URL'

    # DeviceData property
    schedule_property = datastore.DS_schedule_KEY

    # Commands
    check_fluid_command       = 'check_fluid'
    take_measurements_command = 'take_measurements'
    harvest_plant_command     = 'harvest_plant'
    prune_plant_command       = 'prune_plant'

    # Sub keys
    default_repeat_hours_key = 'default_repeat_hours'

    # Commands, there can only be one of each per device.
    commands = {
        check_fluid_command: 
            {message_key: 'Check your fluid level', 
             default_repeat_hours_key: 48,
             URL_key: None},
        take_measurements_command: 
            {message_key: 'Record your plant measurements', 
             default_repeat_hours_key: 24,
             URL_key: None},
        harvest_plant_command: 
            {message_key: 'Time to harvest your plant', 
             default_repeat_hours_key: 0,
             URL_key: None},
        prune_plant_command:
            {message_key: 'Time to prune your plant', 
             default_repeat_hours_key: 48,
             URL_key: 'https://www.youtube.com/watch?v=9noUUTuPh3E'},
#TODO: replace above with Rebekah's video
    }

    # For logging
    name: str = 'cloud_common.cc.notifications.scheduler'


    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        self.__testing_hours = 0


    #--------------------------------------------------------------------------
    # Get the list of commands we support for display.
    def get_commands(self) -> str:
        pp = pprint.PrettyPrinter()
        out = pp.pformat(self.commands)
        return f'{self.name} Commands:\n{out}'


    #--------------------------------------------------------------------------
    # Private getter of the schedule property.
    # Returns a list of dicts.
    def __get_schedule(self, device_ID: str) -> List[Dict[str, str]]:
        return datastore.get_device_data(self.schedule_property, device_ID)


    #--------------------------------------------------------------------------
    # Private command validator.
    # Returns True if command is valid, False otherwise.
    def __validate_command(self, command: str) -> bool:
        if command in self.commands:
            return True
        return False


    #--------------------------------------------------------------------------
    # Return a string of the schedule for a device.  For testing and debugging.
    def to_str(self, device_ID: str) -> str:
        pp = pprint.PrettyPrinter()
        out = pp.pformat(self.__get_schedule(device_ID))
        return out


    #--------------------------------------------------------------------------
    # Get an entity by device ID and command name.
    # Returns the entity if found, or None
    def get_command_entity(self, device_ID: str, command: str) -> Dict[str, str]:
        if not self.__validate_command(command):
            logging.error(f'{self.name}.get_command_entity '
                    f'invalid command {command}')
            return None
        # get list of all entities by time
        entities = datastore.get_sharded_entities(
                datastore.DS_device_data_KIND, 
                self.schedule_property, device_ID)
        if 0 == len(entities):
            return None
        for e in entities:
            # get this entities data property 
            cmd_dict = e.get(datastore.DS_DeviceData_data_Property, {})
            if cmd_dict.get(self.command_key) == command:
                return e
        return None


    #--------------------------------------------------------------------------
    # Creates a DS dict entry for this scheduled command, 
    # setting timestamp = now() + hours and count = 0.
    def add(self, device_ID: str, command: str, repeat_hours: int = -1) -> None:
        if not self.__validate_command(command):
            logging.error(f'{self.name}.add invalid command {command}')
            return

        # customize the command template
        template = self.commands.get(command, {})
        repeat = template.get(self.default_repeat_hours_key, 0)
        if repeat_hours >= 0:
            repeat = repeat_hours

        # calculate when to run this command: now + repeat hours
        utc_in_repeat_hours = dt.datetime.utcnow() + dt.timedelta(hours=repeat)
        run_at_time = utc_in_repeat_hours.strftime('%FT%XZ')

        # create a new command
        cmd_dict = {}
        cmd_dict[self.command_key] = command
        cmd_dict[self.message_key] = template.get(self.message_key, '')
        cmd_dict[self.run_at_key]  = run_at_time
        cmd_dict[self.repeat_key]  = repeat
        cmd_dict[self.count_key]   = 0
        cmd_dict[self.URL_key]     = template.get(self.URL_key, '')

        # update this command dict (remove and add)
        self.update_command(device_ID, cmd_dict)

        # save the command as a new entity
        datastore.save_device_data(device_ID, self.schedule_property, cmd_dict)
        logging.debug(f'{self.name}.added command to schedule: {cmd_dict}')


    #--------------------------------------------------------------------------
    # Create a single notification right away, no scheduling.
    def create_notification(self, device_ID: str, command: str) -> None:
        if not self.__validate_command(command):
            logging.error(f'{self.name}.create_notification invalid '
                    f'command {command}')
            return
        template = self.commands.get(command, {})
        cmd_msg = template.get(self.message_key, '')
        URL = template.get(self.URL_key, '')
        nd = NotificationData()
        nd.add(device_ID, cmd_msg, URL=URL)


    #--------------------------------------------------------------------------
    # Remove a command entity for this device.
    def remove_command(self, device_ID: str, command: str) -> None:
        # get any existing entity for this command and delete it.
        entity = self.get_command_entity(device_ID, command)
        if entity is not None:
            # delete this entity
            DS = datastore.get_client()
            DS.delete(key=entity.key)
        logging.debug(f'{self.name}.remove_command {command}')


    #--------------------------------------------------------------------------
    # Removes all commands for this device.
    def remove_all_commands(self, device_ID: str) -> None:
        # get list of all entities by time
        entities = datastore.get_sharded_entities(
                datastore.DS_device_data_KIND, 
                self.schedule_property, device_ID)
        if 0 == len(entities):
            return None
        DS = datastore.get_client()
        for e in entities:
            DS.delete(key=e.key)
        logging.debug(f'{self.name}.remove_all_commands done.')


    #--------------------------------------------------------------------------
    # Replaces a command in the list.  
    # If the command isn't already in the list, nothing changes.
    def update_command(self, device_ID: str, cmd_dict: Dict[str, str]) -> None:
        cmd_name = cmd_dict.get(self.command_key, None)
        if not self.__validate_command(cmd_name):
            logging.error(f'{self.name}.update_command invalid {cmd_name}')
            return
        # get any existing entity for this command and delete it.
        self.remove_command(device_ID, cmd_name)
        # save the command as a new entity
        datastore.save_device_data(device_ID, self.schedule_property, cmd_dict)
        logging.debug(f'{self.name}.update_command {cmd_dict}')


    #--------------------------------------------------------------------------
    # Set the number of hours, for use when testing check().
    def set_testing_hours(self, hours: int = 0) -> None:
        self.__testing_hours = hours


    #--------------------------------------------------------------------------
    # private internal method: 
    # Execute the command:
    #   Adds notifications to a devices queue.
    def __execute(self, device_ID: str, now: Any, cmd: Dict[str, str]) -> None:
        logging.debug(f'{self.name}.__execute {cmd}')

        cmd_name = cmd.get(self.command_key)
        cmd_msg = cmd.get(self.message_key)
        URL = cmd.get(self.URL_key)

        # All our existing commands just create a notification
        nd = NotificationData()
        nd.add(device_ID, cmd_msg, URL=URL)

        # For the take measurements command, the first repeat time is a week,
        # then it repeats every default (48) hours.
        default_repeat = cmd.get(self.repeat_key)
        if cmd_name == self.take_measurements_command:
            template = self.commands.get(cmd_name, {})
            default_repeat = template.get(self.default_repeat_hours_key, 0)

        # Does this command repeat?
        repeat = cmd.get(self.repeat_key, 0)
        if repeat == 0:
            # No, so remove the command from the schedule.
            self.remove_command(device_ID, cmd_name)
            logging.debug(f'{self.name}.check removed {cmd_name}')
        else:
            # Update the count and next run time.
            cmd[self.count_key] = cmd.get(self.count_key, 0) + 1
            run_at = now + dt.timedelta(hours=default_repeat)
            cmd[self.run_at_key] = run_at.strftime('%FT%XZ')
            # Update this command
            self.update_command(device_ID, cmd)
            logging.debug(f'{self.name}.check updated/replaced {cmd}')


    #--------------------------------------------------------------------------
    # Check the schedule for this device to see if there is anything to run.
    def check(self, device_ID: str) -> None:
        # For testing the schedule without waiting for wall clock time,
        # use the offset externally set to adjust the "now" time.
        now = dt.datetime.utcnow() + dt.timedelta(hours=self.__testing_hours)
        now_str = now.strftime('%FT%XZ')
        logging.debug(f'{self.name}.check '
                f'testing_hours={self.__testing_hours} now={now_str}')

        # Iterate the schedule entries for device_ID acting upon entries that
        # have a timestamp <= now() 
        sched_list = self.__get_schedule(device_ID)
        for cmd in sched_list:
            cmd_name = cmd.get(self.command_key)
            if cmd_name == None:
                continue
            logging.debug(f'{self.name}.checking command={cmd}')
            # Has the command run at time passed?
            if now_str >= cmd.get(self.run_at_key):
                # Yes, so execute it.
                self.__execute(device_ID, now, cmd)




