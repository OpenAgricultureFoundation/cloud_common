# https://google-cloud-python.readthedocs.io/en/stable/datastore/usage.html

import datetime as dt
import uuid, json, logging, time, sys
from typing import Any, List, Dict

from google.cloud import datastore

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars


# Entity types 
DS_last_device_data_KIND = 'LastDeviceData'
DS_device_data_KIND = 'DeviceData'
DS_devices_KIND = 'Devices'
DS_users_KIND = 'Users'
DS_user_session_KIND = 'UserSession'
DS_hort_KIND = 'HorticultureMeasurements'
DS_turds_KIND = 'MqttServiceTurds'
DS_cache_KIND = 'MqttServiceCache'
DS_images_KIND = 'Images'


# Keys for datastore DeviceData entity
DS_device_uuid_KEY = 'device_uuid'
DS_co2_KEY = 'air_carbon_dioxide_ppm'
DS_rh_KEY = 'air_humidity_percent'
DS_temp_KEY = 'air_temperature_celcius'
DS_led_KEY = 'light_spectrum_nm_percent'
DS_led_dist_KEY = 'light_illumination_distance_cm'
DS_led_intensity_KEY = 'light_intensity_watts'
DS_boot_KEY = 'boot'
DS_status_KEY = 'status'  
DS_h20_ec_KEY = 'water_electrical_conductivity_ms_cm'
DS_h20_ph_KEY = 'water_potential_hydrogen'
DS_h20_temp_KEY = 'water_temperature_celcius'
DS_runs_KEY = 'runs'


# How many entries are in the DeviceData queue of dicts.
DS_env_vars_MAX_size = 100000 # maximum number of values in each env. var list
# srmoore wants to test 100K entries Aug. 14, 2019.


# Global
__ds_client = None


#------------------------------------------------------------------------------
# Datastore client for google cloud
def create_client() -> Any:
    logging.debug(f'cloud_common.cc.google.datastore client created.')
    return datastore.Client(env_vars.cloud_project_id)


#------------------------------------------------------------------------------
def get_client() -> Any:
    global __ds_client 
    if __ds_client is None:
        __ds_client = create_client()
    return __ds_client


#------------------------------------------------------------------------------
# Returns dict of counts.
def get_count_of_entities_from_DS():
    res = {}
    res[DS_devices_KIND] = get_entity_count_from_DS(DS_devices_KIND)
    res[DS_device_data_KIND] = get_entity_count_from_DS(DS_device_data_KIND)
    res[DS_users_KIND] = get_entity_count_from_DS(DS_users_KIND)
    res[DS_hort_KIND] = get_entity_count_from_DS(DS_hort_KIND)
    res[DS_cache_KIND] = get_entity_count_from_DS(DS_cache_KIND)
    res[DS_turds_KIND] = get_entity_count_from_DS(DS_turds_KIND)
    res['DeviceDataLastHour'] = get_DeviceData_active_last_hour_count_from_DS()
    res['timestamp'] = dt.datetime.utcnow().strftime('%FT%XZ')
    return res


#------------------------------------------------------------------------------
def get_DeviceData_active_last_hour_count_from_DS():
    DS = get_client()
    if DS is None:
        return 0
    query = DS.query(kind=DS_device_data_KIND)
    entities = list(query.fetch()) # get all entities
    count = 0
    utc = dt.datetime.utcnow() - dt.timedelta(hours=1)
    one_hour_ago = utc.strftime('%FT%XZ')
    for e in entities:
        # status message are sent every 5 min.
        status = e.get(DS_status_KEY, [None])[0]
        #print( e.key.id_or_name )
        if status is not None:
            try:
                ts = status.get('timestamp', b'')
                ts = utils.bytes_to_string(ts)
                if ts > one_hour_ago: 
                    count = count + 1
            except:
                pass
    
#<Entity('DeviceData', 'EDU-11A8F684-f4-5e-ab-67-5d-ad') {'status': [<Entity {'timestamp': b'2018-11-16T15:58:07Z', 'value': b'{"timestamp": "2018-11-16T15:58:07Z", "IP": "10.33.136.13", "package_version": "1.0-4", "device_config": "edu-v0.3.0", "status": "OK", "internet_connection": "True", "memory_available": "98M", "disk_available": "945M", "iot_status": "2018-11-16-T15:46:42Z", "iot_received_message_count": 2, "iot_published_message_count": 2, "recipe_percent_complete": 33.199074074074076, "recipe_percent_complete_string": "33.20 %", "recipe_time_remaining_minutes": 28858, "recipe_time_remaining_string": "20 Days 0 Hours 58 Minutes", "recipe_time_elapsed_string": "9 Days 23 Hours 2 Minutes"}', 'name': b'None'}>,

    return count


#------------------------------------------------------------------------------
def get_entity_count_from_DS(kind):
    DS = get_client()
    if DS is None:
        return 0
    query = DS.query(kind=kind)
    query.keys_only() # retuns less data, so faster
    entities = list(query.fetch()) # get all entities (keys only)
    return len(entities)


#------------------------------------------------------------------------------
def get_one_from_DS(kind, key, value):
    DS = get_client()
    if DS is None:
        return None
    query = DS.query(kind=kind)
    query.add_filter(key, '=', value)
    result = list(query.fetch(1)) # just get the first one (no order)
    if not result:
        return None
    return result[0]


#------------------------------------------------------------------------------
def get_all_from_DS(kind, key, value):
    DS = get_client()
    if DS is None:
        return []
    query = DS.query(kind=kind)
    query.add_filter(key, '=', value)
    result = list(query.fetch()) # fetch all data
    if not result:
        return []
    return result


#------------------------------------------------------------------------------
# Return the entity indexed by our custom key (usually the device ID).
def get_by_key_from_DS(kind, key):
    DS = get_client()
    if DS is None:
        return None
    _key = DS.key(kind, key)
    _ent = DS.get(_key)
    if not _ent: 
        return None
    return _ent


#------------------------------------------------------------------------------
# Return a list of the entity keys for the specified kind.
# Useful when you custom key the entity with a device ID.
def get_keys(kind) -> List[str]:
    DS = get_client()
    if DS is None:
        return []
    query = DS.query(kind=kind)
    query.keys_only() # retuns less data, so faster
    entities = list(query.fetch()) # get all entities (keys only)
    keys = []
    for ent in entities:
        keys.append(ent.key.id_or_name)
    return keys


#------------------------------------------------------------------------------
# Save a custom keyed entity.  
# Returns True / False.
def save_with_key(kind: str, key: str, data: str) -> bool:
    try:
        DS = get_client()
        if DS is None:
            return False

        # Get this device data from the datastore (or create an empty one).
        # These DeviceData entities are custom keyed with our device_ID.
        ddkey = DS.key(kind, key)
        dd = DS.get(ddkey) 
        if not dd: 
            # The entity doesn't exist, so create it (no transaction needed)
            dd = datastore.Entity(ddkey)
            dd.update({}) 
        dd[key] = data
        DS.put(dd)

        logging.info(f'save_with_key: kind={kind} key={key} data={data}')
        return True

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical(f'Exception in save_with_key(): {e}')
        traceback.print_tb( exc_traceback, file=sys.stdout )
        return False


#------------------------------------------------------------------------------
def get_device_name_from_DS(device_uuid):
    DS = get_client()
    if DS is None:
        return "error"
    query = DS.query(kind=DS_devices_KIND)
    query.add_filter('device_uuid', '=', device_uuid)
    results = list(query.fetch(1)) # just get first (no order)
    if len(results) > 0:
        return results[0]["device_name"]
    else:
        return "Invalid device"


#------------------------------------------------------------------------------
# Get device data status
def get_device_data_from_DS(device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return None

    device_data = get_by_key_from_DS(DS_device_data_KIND, device_uuid)
    if device_data is None:
        return None

    air_temperature_celcius = device_data.get(DS_temp_KEY, [{}])[0].get("value", '')
    status = device_data.get(DS_status_KEY, [None])[0]

    result_json = {
        "timestamp": status.get("timestamp", ""),
        "percent_complete": status.get("recipe_percent_complete_string", ""),
        "time_elapsed": status.get("recipe_time_elapsed_string", ""),
        "device_status": status.get("status", ""),
        "air_temp": air_temperature_celcius
    }
    return result_json


#------------------------------------------------------------------------------
def get_count_of_users_devices_from_DS(user_uuid):
    DS = get_client()
    if DS is None:
        return 0
    query = DS.query(kind=DS_devices_KIND)
    query.keys_only() # retuns less data, so faster
    query.add_filter('user_uuid', '=', user_uuid)
    entities = list(query.fetch()) # get all entities (keys only)
    return len(entities)


#------------------------------------------------------------------------------
def get_list_of_users_from_DS():
    res = {}
    DS = get_client()
    if DS is None:
        return res
    res['users'] = [] # list of users
    query = DS.query(kind=DS_users_KIND)
    users = list(query.fetch()) # get all users
    for u in users:
        user = {}
        da = u.get('date_added', '')
        user["account_creation_date"] = da.strftime('%FT%XZ')
        user["email_address"] = u.get('email_address', '')
        user["user_name"] = u.get('username', '')
        user["user_uuid"] = u.get('user_uuid', '')
        user["organization"] = u.get('organization', '')

        user["number_of_devices"] = get_count_of_users_devices_from_DS(
                user["user_uuid"])

        user["account_activity_date"] = 'Never Active'
        adate = get_latest_user_session_created_date_from_DS(user["user_uuid"])
        if adate is not None:
            user["account_activity_date"] = adate

        res['users'].append(user)

    res['timestamp'] = dt.datetime.utcnow().strftime('%FT%XZ')
    return res


#------------------------------------------------------------------------------
def get_list_of_devices_from_DS():
    res = {}
    DS = get_client()
    if DS is None:
        return res
    res['devices'] = [] # list of devices
    query = DS.query(kind=DS_devices_KIND)
    devices = list(query.fetch()) # get all devices 
    for d in devices:
        device = {}
        rd = d.get('registration_date', None) # web ui reg date
        if rd is None:
            device['registration_date'] = ''
        else:
            device['registration_date'] = rd.strftime('%FT%XZ') 
        device['device_name'] = d.get('device_name', '')
        device['device_notes'] = d.get('device_notes', '')
        device_uuid = d.get('device_uuid', '')
        device['device_uuid'] = device_uuid
        user_uuid = d.get('user_uuid', '')
        device['user_uuid'] = user_uuid
        device['last_config_send_time'] = 'Never' # in case no IoT device
        device['last_error_message'] = 'No IoT registration'
        device['user_name'] = 'None'
        if 0 != len(user_uuid):
            user = get_one_from_DS(DS_users_KIND, 'user_uuid', user_uuid)
            if user is not None:
                device['user_name'] = user.get('username','None')

        device['remote_URL'] = ''
        device['access_point'] = ''
        if 0 < len(device_uuid):
            dd = get_by_key_from_DS(DS_device_data_KIND, device_uuid)
            if dd is not None and DS_boot_KEY in dd:
                boot = dd.get(DS_boot_KEY) # list of boot messages

                # get latest boot message
                last_boot = boot[0].get('value')

                # convert binary into string and then a dict
                boot_dict = json.loads(utils.bytes_to_string(last_boot))

                # the serveo link needs to be lower case
                remote_URL = boot_dict.get('remote_URL')
                if remote_URL is not None:
                    remote_URL = remote_URL.lower()
                    device['remote_URL'] = remote_URL

                # get the AP
                access_point = boot_dict.get('access_point')
                if access_point is not None:
                    # extract just the wifi code
                    if access_point.startswith('BeagleBone-'):
                        ap = access_point.split('-')
                        if 2 <= len(ap):
                            access_point = ap[1]
                            device['access_point'] = access_point

        res['devices'].append(device)

    res['timestamp'] = dt.datetime.utcnow().strftime('%FT%XZ')
    return res


#------------------------------------------------------------------------------
def get_list_of_device_data_from_DS():
    res = {}
    DS = get_client()
    if DS is None:
        return res
    res['devices'] = [] # list of devices with data from each
    query = DS.query(kind=DS_devices_KIND)
    devices = list(query.fetch()) # get all devices 
    for d in devices:
        device = {}

        device_uuid = d.get('device_uuid', '')
        device['device_uuid'] = device_uuid

        device['device_name'] = d.get('device_name', '')

        user_uuid = d.get('user_uuid', '')
        device['user_name'] = user_uuid
        if 0 != len(user_uuid):
            user = get_one_from_DS(DS_users_KIND, 'user_uuid', user_uuid)
            if user is not None:
                device['user_name'] = user.get('username','None')

        # Get the DeviceData for this device ID
        dd = None
        if 0 < len(device_uuid):
            dd = get_by_key_from_DS(DS_device_data_KIND, device_uuid)
        
        device['remote_URL'] = ''
        device['access_point'] = ''
        if dd is not None and DS_boot_KEY in dd:
            boot = dd.get(DS_boot_KEY) # list of boot messages

            # get latest boot message
            last_boot = boot[0].get('value')

            # convert binary into string and then a dict
            boot_dict = json.loads(utils.bytes_to_string(last_boot))

            # the serveo link needs to be lower case
            remote_URL = boot_dict.get('remote_URL')
            if remote_URL is not None:
                remote_URL = remote_URL.lower()
                device['remote_URL'] = remote_URL

            # get the AP
            access_point = boot_dict.get('access_point')
            if access_point is not None:
                # extract just the wifi code
                if access_point.startswith('BeagleBone-'):
                    ap = access_point.split('-')
                    if 2 <= len(ap):
                        access_point = ap[1]
                        device['access_point'] = access_point

        epoch = '1970-01-01T00:00:00Z'
        last_message_time = epoch
        val, ts = get_latest_val_from_DeviceData(dd, DS_rh_KEY)
        device[DS_rh_KEY] = val
        if ts > last_message_time:
            last_message_time = ts

        val, ts = get_latest_val_from_DeviceData(dd, DS_temp_KEY)
        device[DS_temp_KEY] = val
        if ts > last_message_time:
            last_message_time = ts

        val, ts = get_latest_val_from_DeviceData(dd, DS_co2_KEY)
        device[DS_co2_KEY] = val
        if ts > last_message_time:
            last_message_time = ts

        val, ts = get_latest_val_from_DeviceData(dd, DS_h20_ec_KEY)
        device[DS_h20_ec_KEY] = val
        if ts > last_message_time:
            last_message_time = ts

        val, ts = get_latest_val_from_DeviceData(dd, DS_h20_ph_KEY)
        device[DS_h20_ph_KEY] = val
        if ts > last_message_time:
            last_message_time = ts

        val, ts = get_latest_val_from_DeviceData(dd, DS_h20_temp_KEY)
        device[DS_h20_temp_KEY] = val
        if ts > last_message_time:
            last_message_time = ts

        if last_message_time == epoch:
            last_message_time = 'Never'
        device['last_message_time'] = last_message_time 

        device['stale'] = get_minutes_since_UTC_timestamp(last_message_time)

        device['last_image'] = get_latest_image_URL(device_uuid)

        res['devices'].append(device)

    res['timestamp'] = dt.datetime.utcnow().strftime('%FT%XZ')
    return res


#------------------------------------------------------------------------------
# Returns '' for failure or the latest URL published by this device.
def get_latest_image_URL(device_uuid):
    URL = ''
    DS = get_client()
    if DS is None:
        return URL

    # Sort by date descending and take the first 50
    # This is equivalent to taking the most recent 50 images
    # Then, reverse the order so it's chronological
    image_query = DS.query(kind=DS_images_KIND,
                                         order=['-creation_date'])
    image_query.add_filter('device_uuid', '=', device_uuid)

    image_list = list(image_query.fetch(1))[::-1]
    if not image_list:
        return URL

    image_entity = image_list[0]
    if not image_entity:
        return URL
    URL = decode_url(image_entity)
    return URL


#------------------------------------------------------------------------------
def decode_url(image_entity):
    url = image_entity.get('URL', '')
    return utils.bytes_to_string(url)


#------------------------------------------------------------------------------
# Returns the value, timestamp if the key exists
def get_latest_val_from_DeviceData(dd, key):
    if dd is None or key not in dd:
        return '', ''
    valsList = dd.get(key, []) # list of values
    # return latest value and timestamp
    value = valsList[0].get('value', b'')
    value = utils.bytes_to_string(value) # could be bytes, so decode

    ts = valsList[0].get('timestamp', b'') 
    ts = utils.bytes_to_string(ts) # could be bytes, so decode
    return value, ts


#------------------------------------------------------------------------------
# Pass in a UTC timestamp string to find out if it is within the past 15 min.
# Returns the minutes as a string, e.g. '60'
def get_minutes_since_UTC_timestamp(ts):
    if ts == 'Never':
        return ts
    now = dt.datetime.utcnow()
    ts = dt.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ') # string to dt obj
    delta = now - ts
    minutes = delta.total_seconds() / 60.0
    return "{}".format(int(minutes))


#------------------------------------------------------------------------------
def get_latest_user_session_created_date_from_DS(user_uuid):
    DS = get_client()
    if DS is None:
        return None
    sessions = get_all_from_DS(DS_user_session_KIND, 'user_uuid', user_uuid)
    if sessions is None or 0 == len(sessions):
        return None
    dates = []
    for s in sessions:
        dates.append(s.get('created_date', '').strftime('%FT%XZ'))
    dates.sort(reverse=True) # sort descending, newest date on top

    # delete all the old (stale) sessions that are not the latest
    for s in sessions:
        if dates[0] != s.get('created_date', '').strftime('%FT%XZ'):
            DS.delete(key=s.key)

    return dates[0] # return the latest date (top of array)


#------------------------------------------------------------------------------
# Returns True for delete or False for error.
def delete_user_from_DS(user_uuid):
    DS = get_client()
    if DS is None:
        return False
    user = get_one_from_DS(DS_users_KIND, 'user_uuid', user_uuid)
    if user is None:
        return False
    DS.delete(key=user.key)
    return True


#------------------------------------------------------------------------------
# Adds a (test) user to the DS (so we can test deleting in the admin UI).
def add_user_to_DS(username, email_address, organization):
    DS = get_client()
    if DS is None:
        return None
    key = DS.key(DS_users_KIND)
    user_uuid = str(uuid.uuid4())
    add_user_task = datastore.Entity(key, exclude_from_indexes=[])
    add_user_task.update({
        'username': username,
        'email_address': email_address,
        'password': '',
        'date_added': dt.datetime.utcnow(),
        'organization': organization,
        'user_uuid': user_uuid,
        'is_verified': True,
    })
    DS.put(add_user_task)
    if add_user_task.key:
        return user_uuid
    return None


#------------------------------------------------------------------------------
# Update a user's profile.
def update_user(user_uuid, username, email_address, organization):
    try:
        DS = get_client()
        query = DS.query(kind='Users')
        query.add_filter('user_uuid', '=', user_uuid)
        user = list(query.fetch(1))[0]
        # 'testman@mailinator.com'
        user['email_address'] = email_address
        user['username']      = username
        user['organization']  = organization
        DS.put(user)
        return True
    except:
        return False


#------------------------------------------------------------------------------
# Delete a device_uuid from the Devices and DeviceData entity collections.
# Returns True.
def delete_device_from_DS(device_uuid):
    DS = get_client()
    if DS is None:
        return False
    device = get_one_from_DS(DS_devices_KIND, 'device_uuid', device_uuid)
    if device is not None:
        DS.delete(key=device.key)

    device_data = get_by_key_from_DS(DS_device_data_KIND, device_uuid)
    if device_data is not None:
        DS.delete(key=device_data.key)

    # This entity does not exist as of 2019-01-07 but will when Rob adds it to
    # the MQTT service.  This call will just return None until then.
    last_device_data = get_by_key_from_DS(DS_last_device_data_KIND, device_uuid)
    if last_device_data is not None:
        DS.delete(key=last_device_data.key)

    return True


#------------------------------------------------------------------------------
# Adds a (test) device to the DS (so we can test deleting in the admin UI).
def add_device_to_DS(device_name, device_notes):
    DS = get_client()
    if DS is None:
        return None
    key = DS.key(DS_devices_KIND)
    device_uuid = str(uuid.uuid4())
    add_device_task = datastore.Entity(key, exclude_from_indexes=[])
    add_device_task.update({
        'device_name': device_name,
        'device_notes': device_notes,
        'device_type': 'EDU',
        'registration_date': dt.datetime.utcnow(),
        'device_uuid': device_uuid,
        'user_uuid': str(uuid.uuid4()),
    })
    DS.put(add_device_task)
    if add_device_task.key:
        return device_uuid
    return None


#------------------------------------------------------------------------------
# Get the entity property list of dicts.
# Returns a list of dicts.
def get_entity_property(entity_kind: str, entity_key: str, 
        property_name: str) -> List[Dict[str, str]]:
    if entity_key is None or entity_key is 'None' or \
            property_name is None or property_name is 'None':
        return [{}]

    dd = get_by_key_from_DS(entity_kind, entity_key)
    if dd is None:
        return [{}]

    return dd.get(property_name, [{}])


#------------------------------------------------------------------------------
# Get the DeviceData property list of dicts.
# Returns a list of dicts.
def get_device_data_property(device_ID: str, 
        property_name: str) -> List[Dict[str, str]]:
    return get_entity_property(DS_device_data_KIND, device_ID, property_name)


#------------------------------------------------------------------------------
# Save a bounded list of data to the entity by key and property.
# A cache for UI display / charting.
def push_dict_onto_entity_queue(entity_kind: str, entity_key: str, 
        property_name: str, pydict: Dict, 
        max_list_size: int = DS_env_vars_MAX_size) -> bool:
    try:
        DS = get_client()
        if DS is None:
            return False

        # Get this entity from the datastore (or create an empty one).
        # These entities are custom keyed with an entity_key.
        ddkey = DS.key(entity_kind, entity_key)
        dd = DS.get(ddkey) 
        if not dd: 
            # The device data entity doesn't exist, so create it
            dd = datastore.Entity(ddkey)
            dd.update({})   # empty entity
            DS.put(dd)      # write to DS

        # retry the Entity update in a transaction until it succeeds
        transactionWorked = False
        for _ in range(15):
            try:
                with DS.transaction():
                    dd = DS.get(ddkey)

                    # get a property named for the env var, which is a list of
                    # dict values
                    valuesList = dd.get(property_name, [])

                    # put this value at the front of the list
                    valuesList.insert(0, pydict)
                    # cap max size of list
                    while len(valuesList) > max_list_size:
                        valuesList.pop() # remove last item in list

                    # update the entity
                    dd[property_name] = valuesList 

                    # save the entity to the datastore
                    dd.exclude_from_indexes = dd.keys()
                    DS.put(dd)  
                    transactionWorked = True
                    break
            except Exception as e:
                #logging.debug('save_data_to_Device: transaction failed '\
                #        '{}'.format( e ))
                continue
        if not transactionWorked:
            logging.error(f'push_dict_onto_entity_queue: '
                    f'transaction failed for entity={entity_kind} '
                    f'entity_key={entity_key} name={property_name}')
            return False

        logging.debug(f'push_dict_onto_entity_queue: saved '
                f'entity={entity_kind} entity_key={entity_key} '
                f'name={property_name} dict={pydict}')
        return True

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical( "Exception in push_dict_onto_entity_queue(): %s" % e)
        traceback.print_tb( exc_traceback, file=sys.stdout )
        return False


#------------------------------------------------------------------------------
# Save a bounded list of the recent values of each env. var. to the Device
# that produced them - for UI display / charting.
def push_dict_onto_device_data_queue(device_ID: str, 
        property_name: str, pydict: Dict) -> bool:
    return push_dict_onto_entity_queue(DS_device_data_KIND, device_ID, 
            property_name, pydict)


#------------------------------------------------------------------------------
# Save a list of dicts to the DeviceData property.
def save_list_as_device_data_queue(device_ID: str, 
        property_name: str, pylist: List) -> bool:
    try:
        DS = get_client()
        if DS is None:
            return False

        # Get this device data from the datastore (or create an empty one).
        # These DeviceData entities are custom keyed with our device_ID.
        ddkey = DS.key(DS_device_data_KIND, device_ID)
        dd = DS.get(ddkey) 
        if not dd: 
            # The device data entity doesn't exist, so create it 
            # (no transaction needed)
            dd = datastore.Entity(ddkey)
            dd.update({})   # empty entity
            DS.put(dd)      # write to DS

        # retry the Entity update in a transaction until it succeeds
        transactionWorked = False
        for _ in range(15):
            try:
                with DS.transaction():
                    dd = DS.get(ddkey)
                    dd[property_name] = pylist 
                    dd.exclude_from_indexes = dd.keys()
                    DS.put(dd)  
                    transactionWorked = True
                    break
            except Exception as e:
                #logging.debug('save_data_to_Device: transaction failed '\
                #        '{}'.format( e ))
                continue
        if not transactionWorked:
            logging.error(f'save_list_as_device_data_queue: '
                    f'transaction failed '
                    f'for device_ID={device_ID} name={property_name}')
            return False

        logging.debug(f'save_list_as_device_data_queue: saved '
                f'device_ID={device_ID} name={property_name} list={pylist}')
        return True

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical(f'Exception in save_list_as_device_data_queue(): {e}')
        traceback.print_tb( exc_traceback, file=sys.stdout )
        return False


#------------------------------------------------------------------------------
# Save the URL to an image in cloud storage, as an entity in the datastore, 
# so the UI can fetch it for display / time lapse.
def saveImageURL(deviceId, publicURL, cameraName):
    DS = get_client()
    if DS is None:
        return 
    key = DS.key(DS_images_KIND)
    image = datastore.Entity(key, exclude_from_indexes=[])
    cd = time.strftime( '%FT%XZ', time.gmtime())
    # Don't use a dict, the strings will be assumed to be "blob" and will be
    # shown as base64 in the console.
    # Use the Entity like a dict to get proper strings.
    image['device_uuid'] = deviceId
    image['URL'] = publicURL
    image['camera_name'] = cameraName
    image['creation_date'] = cd
    DS.put(image)  
    logging.info("datastore.saveImageURL: saved {}".format( image ))
    return 


#------------------------------------------------------------------------------
def get_device_name(device_uuid):
    DS = get_client()
    if DS is None:
        return 
    query = DS.query(kind='Devices')
    query.add_filter('device_uuid', '=', device_uuid)
    results = list(query.fetch())
    if len(results) > 0:
        return results[0]["device_name"]
    else:
        return "Invalid device"

#------------------------------------------------------------------------------
# Returns None for not found or version never set, otherwise a string version.
def get_device_software_version(device_uuid):
    DS = get_client()
    if DS is None:
        return 
    query = DS.query(kind='Devices')
    query.add_filter('device_uuid', '=', device_uuid)
    results = list(query.fetch())
    if len(results) == 0:
        return None
    return results[0].get("device_software_version", None)




