# https://google-cloud-python.readthedocs.io/en/stable/bigquery/usage.html

# BigQuery is SLOW.  Only use it for research queries and warn the user.
# Note: most data from the device is cached in the datastore.

import ast, logging, json
from google.cloud import bigquery

from cloud_common.cc.google import env_vars

# This should be the only place we store queries.
from cloud_common.cc.google import queries

bigquery_client = bigquery.Client()


# ------------------------------------------------------------------------------
# Get a dict with two arrays of the temp and humidity historical values.
# Returns a dict.
def get_temp_and_humidity_history_from_BQ(device_uuid):
    humidity_array = []
    temp_array = []
    result_json = {
        'RH': humidity_array,
        'temp': temp_array
    }
    if device_uuid is None or device_uuid is 'None':
        return result_json

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_str = queries.formatQuery(
        queries.fetch_temp_results_history, device_uuid)
    query_job = bigquery_client.query(query_str, job_config=job_config)
    query_result = query_job.result()
    for row in list(query_result):
        rvalues = row[2]  # can't use row.values
        values_json = (ast.literal_eval(rvalues))

        if 'air_temperature_celcius' == row.var and 'values' in values_json:
            values = values_json["values"]
            result_json["temp"].append(
                {'value': values[0]['value'], 'time': row.eastern_time})

        if 'air_humidity_percent' == row.var and 'values' in values_json:
            values = values_json["values"]
            result_json["RH"].append(
                {'value': values[0]['value'], 'time': row.eastern_time})
    return result_json


#------------------------------------------------------------------------------
# Insert data into our bigquery dataset and table.
def data_insert(rowsList):
    try:
        logging.info("bq insert rows: {}".format(rowsList))

        dataset_ref = bigquery_client.dataset(env_vars.bq_dataset, 
                project=env_vars.cloud_project_id)
        table_ref = dataset_ref.table(env_vars.bq_table)
        table = bigquery_client.get_table(table_ref )               

        response = bigquery_client.insert_rows(table, rowsList)
        logging.debug('bq response: {}'.format(response ))

        return True

    except Exception as e:
        logging.critical( "bigquery.data_insert: Exception: %s" % e )
        return False


#--------------------------------------------------------------------------
# Save the data to BQ.  
# Inserts a new row with an ID made up from the first three arguments: 
#   <data_type>~<device_name>~<timestamp>
def save(data_type: str, device_name: str, timestamp: str, data: dict) -> bool:
    try:
        if data_type is None or device_name is None or data is None:
            logging.error(f'bigquery.save: invalid args.')
            return False
        data_type = data_type.replace( '~', '' ) 
        device_name = device_name.replace( '~', '' ) 
        ID = data_type + '~' + device_name + '~' + timestamp
        rowsList = []
        rowsList.append((ID, json.dumps(data)))
        data_insert(rowsList)
        return True
    except Exception as e:
        logging.error(f'bigquery.save: {e}')
        return False


#------------------------------------------------------------------------------
# Get the counts of types of data values in the public repo.
# Returns a dict of the queried names and values.
def get_total_count_of_data_values_from_BQ():
    res = {}
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = bigquery_client.query(queries.counts, job_config=job_config)
    query_result = query_job.result()
    for row in list(query_result):
        vals = list(row.items())
        for val in vals:
            res[ val[0] ] = "{:,}".format(int(val[1])) # counts are ints
    return res


