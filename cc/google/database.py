# All common database code.  

from cloud_common.cc import utils 
from cloud_common.cc.google import datastore


# ------------------------------------------------------------------------------
# Get the historical CO2 values for this device.  
# Returns a list.
def get_co2_history(device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return []

    device_data = datastore.get_by_key_from_DS(
            datastore.DS_device_data_KIND, device_uuid)
    if device_data is None or datastore.DS_co2_KEY not in device_data:
        return []

    results = []
    valuesList = device_data[datastore.DS_co2_KEY]
    for val in valuesList:
        ts = utils.bytes_to_string(val['timestamp'])
        value = utils.bytes_to_string(val['value'])
        results.append({'value': value, 'time': ts})
    return results


# ------------------------------------------------------------------------------
# Get a list of the led panel historical values.
# Returns a list.
def get_led_panel_history(device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return []

    device_data = datastore.get_by_key_from_DS(
            datastore.DS_device_data_KIND, device_uuid)
    if device_data is None or DS_led_KEY not in device_data:
        return []

    results = []
    valuesList = device_data[DS_led_KEY]
    for val in valuesList:
        led_json = utils.bytes_to_string(val['value'])
        results.append(led_json)
    return results


# ------------------------------------------------------------------------------
# Get a dict with two arrays of the temp and humidity historical values.
# Returns a dict.
def get_temp_and_humidity_history(device_uuid):
    humidity_array = []
    temp_array = []
    result_json = {
        'RH': humidity_array,
        'temp': temp_array
    }
    if device_uuid is None or device_uuid is 'None':
        return result_json

    device_data = datastore.get_by_key_from_DS(
            datastore.DS_device_data_KIND, device_uuid)
    if device_data is None or \
            (datastore.DS_temp_KEY not in device_data and \
             datastore.DS_rh_KEY not in device_data):
        return result_json

    # Get temp values
    if datastore.DS_temp_KEY in device_data:
        valuesList = device_data[datastore.DS_temp_KEY]
        for val in valuesList:
            ts = utils.bytes_to_string(val['timestamp'])
            value = utils.bytes_to_string(val['value'])
            result_json["temp"].append({'value': value, 'time': ts})

    # Get RH values
    if datastore.DS_rh_KEY in device_data:
        valuesList = device_data[datastore.DS_rh_KEY]
        for val in valuesList:
            ts = utils.bytes_to_string(val['timestamp'])
            value = utils.bytes_to_string(val['value'])
            result_json["RH"].append({'value': value, 'time': ts})

    return result_json


# ------------------------------------------------------------------------------
# Generic function to return a float value from DeviceData[key]
def get_current_float_value_from_DS(key, device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return None

    device_data = datastore.get_by_key_from_DS(
            datastore.DS_device_data_KIND, device_uuid)
    if device_data is None or key not in device_data:
        return None

    # process the vars list from the DS into the same format as BQ
    result = None
    valuesList = device_data[key]
    val = valuesList[0]  # the first item in the list is most recent
    result = "{0:.2f}".format(float(val['value']))
    return result


# ------------------------------------------------------------------------------
# Get the current CO2 value for this device.  
# Returns a float or None.
def get_current_CO2_value(device_uuid):
    return get_current_float_value_from_DS(datastore.DS_co2_KEY, device_uuid)


# ------------------------------------------------------------------------------
# Get the current temp value for this device.
# Returns a float or None.
def get_current_temp_value(device_uuid):
    return get_current_float_value_from_DS(datastore.DS_temp_KEY, device_uuid)


# ------------------------------------------------------------------------------
# Get the current RH value for this device.
# Returns a float or None.
def get_current_RH_value(device_uuid):
    return get_current_float_value_from_DS(datastore.DS_rh_KEY, device_uuid)

def get_current_EC_value(device_uuid):
    return get_current_float_value_from_DS(datastore.DS_h20_ec_KEY, device_uuid)

def get_current_pH_value(device_uuid):
    return get_current_float_value_from_DS(datastore.DS_h20_ph_KEY, device_uuid)

def get_current_h2o_temp_value(device_uuid):
    return get_current_float_value_from_DS(datastore.DS_h20_temp_KEY, device_uuid)


