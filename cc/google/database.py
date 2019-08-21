# All common database code.  

from datetime import datetime as dt

from cloud_common.cc import utils 
from cloud_common.cc.google import datastore


# ------------------------------------------------------------------------------
# Get the historical Temp, Humidity, CO2, leaf count, plant height values as
# time series in a date range for this device.  
# Returns 5 lists of dicts: temp, RH, co2, leaf_count, plant_height
def get_all_historical_values(device_uuid, start_timestamp, end_timestamp):
    co2 = [] 
    temp = [] 
    RH = [] 
    leaf_count = [] 
    plant_height = []

    if device_uuid is None or device_uuid is 'None':
        print(f'get_all_historical_values: No device_uuid')
        return temp, RH, co2, leaf_count, plant_height

    co2_vals = datastore.get_device_data(datastore.DS_co2_KEY, device_uuid, 
            count=1000)
    temp_vals = datastore.get_device_data(datastore.DS_temp_KEY, device_uuid, 
            count=1000)
    rh_vals = datastore.get_device_data(datastore.DS_rh_KEY, device_uuid, 
            count=1000)
    if 0 == len(co2_vals) or 0 == len(temp_vals) or 0 == len(rh_vals):
        print(f'get_all_historical_values: No DeviceData for {device_uuid}')
        return temp, RH, co2, leaf_count, plant_height

    # handle None values for date range, in which case we return all 
    start, end = None, None
    try:
        start = dt.strptime(start_timestamp, '%Y-%m-%dT%H:%M:%SZ')
        end = dt.strptime(end_timestamp, '%Y-%m-%dT%H:%M:%SZ')
        print(f'get_all_historical_values: using date range: {str(start)} to {str(end)}')
    except:
        start, end = None, None
        print(f'get_all_historical_values: no date range')

    # make sure the time column is the first entry in each dict
    for val in co2_vals:
        ts_str = utils.bytes_to_string(val['timestamp'])
        ts = dt.strptime(ts_str, '%Y-%m-%dT%H:%M:%SZ')
        if start is not None and end is not None and \
                (ts < start or ts > end):
            continue    # this value is not in our start / end range
        value = utils.bytes_to_string(val['value'])
        co2.append({'time': ts_str, 'value': value})

    for val in temp_vals:
        ts_str = utils.bytes_to_string(val['timestamp'])
        ts = dt.strptime(ts_str, '%Y-%m-%dT%H:%M:%SZ')
        if start is not None and end is not None and \
                (ts < start or ts > end):
            continue    # this value is not in our start / end range
        value = utils.bytes_to_string(val['value'])
        temp.append({'time': ts_str, 'value': value})

    for val in rh_vals:
        ts_str = utils.bytes_to_string(val['timestamp'])
        ts = dt.strptime(ts_str, '%Y-%m-%dT%H:%M:%SZ')
        if start is not None and end is not None and \
                (ts < start or ts > end):
            continue    # this value is not in our start / end range
        value = utils.bytes_to_string(val['value'])
        RH.append({'time': ts_str, 'value': value})

    # get horticulture measurements: leaf_count, plant_height
    query = datastore.get_client().query(kind='DailyHorticultureLog')
    query.add_filter('device_uuid', '=', device_uuid)
    query_result = list(query.fetch())
    if 0 < len(query_result):
        for result in query_result:
            ts_str = str(utils.bytes_to_string(result["submitted_at"]))
            ts = dt.strptime(ts_str, '%Y-%m-%dT%H:%M:%SZ')
            if start is not None and end is not None and \
                    (ts < start or ts > end):
                continue    # this value is not in our start / end range
            leaf_count.append({'time': ts_str, 
                'value': result["leaf_count"]})
            plant_height.append({'time': ts_str, 
                'value': result["plant_height"]})

    return temp, RH, co2, leaf_count, plant_height


# ------------------------------------------------------------------------------
# Get the historical CO2 values for this device.  
# Returns a list.
def get_co2_history(device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return []

    co2_vals = datastore.get_device_data(datastore.DS_co2_KEY, device_uuid, 
            count=1000)
    if 0 == len(co2_vals):
        return []

    results = []
    for val in co2_vals:
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

    led_vals = datastore.get_device_data(datastore.DS_led_KEY, device_uuid, 
            count=1000)
    if 0 == len(led_vals):
        return []

    results = []
    for val in led_vals:
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

    temp_vals = datastore.get_device_data(datastore.DS_temp_KEY, device_uuid, 
            count=1000)
    rh_vals = datastore.get_device_data(datastore.DS_rh_KEY, device_uuid, 
            count=1000)
    if 0 == len(temp_vals) or 0 == len(rh_vals):
        return result_json

    for val in temp_vals:
        ts = utils.bytes_to_string(val['timestamp'])
        value = utils.bytes_to_string(val['value'])
        result_json["temp"].append({'value': value, 'time': ts})

    for val in rh_vals:
        ts = utils.bytes_to_string(val['timestamp'])
        value = utils.bytes_to_string(val['value'])
        result_json["RH"].append({'value': value, 'time': ts})

    return result_json


# ------------------------------------------------------------------------------
# Generic function to return a float value from DeviceData[key]
def get_current_float_value_from_DS(key, device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return ''

    vals = datastore.get_device_data(key, device_uuid, count=1)
    if 0 == len(vals):
        return ''

    # process the vars list from the DS into the same format as BQ
    result = ''
    val = vals[0]  # the first item in the list is most recent
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


