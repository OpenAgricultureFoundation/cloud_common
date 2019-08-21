#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../../.. && pwd )"
cd $DIR # Go to the project top level dir.

if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
  source $DIR/config/gcloud_env.bash
fi

if ! [ -d pyenv ]; then
  echo 'ERROR: you have not run ./scripts/local_development_one_time_setup.sh'
  exit 1
fi

source pyenv/bin/activate

export PYTHONPATH=$DIR

# Run our entry point:
python3.6 -c '
from cloud_common.cc.google import datastore

"""
d = {"value":"24.0","name":"SHT25-Top","timestamp":"2019-07-18T22:57:01Z"}
ret = datastore.save_device_data("debugrobID", "tempC", d)
print(f"save returned {ret}\n")

ret = datastore.get_device_data("tempC", "debugrobID")
print(f"get all returned {len(ret)} rows in the list\n")
print(f"get all {ret}\n")

ret = datastore.get_device_data("tempC", "debugrobID", count=1)
print(f"get single returned {len(ret)} rows in the list\n")
print(f"get latest single {ret}\n")

first_row = ret[0]
data = first_row.get("data")
print(f"data={data}\n")
name = data.get("name")
value = data.get("value")
print(f"name={name}\n")
print(f"value={value}\n")

ts = first_row.get("timestamp")
print(f"ts={ts}\n")

'
