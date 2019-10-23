# https://cloud.google.com/iot/docs/samples/device-manager-samples

import json
import base64
import time
import datetime as dt
from google.oauth2 import service_account
from googleapiclient import discovery, errors

from cloud_common.cc.google import env_vars
from cloud_common.cc.google.firebase import fs_client


class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class SendCommandError(Error):
    """Exception raised for iot command errors.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


# Returns an authorized API client by discovering the IoT API
# using the service account credentials JSON file.
def get_IoT_client(path_to_service_account_json):
    api_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    api_version = "v1"
    discovery_api = "https://cloudiot.googleapis.com/$discovery/rest"
    service_name = "cloudiotcore"

    creds = service_account.Credentials.from_service_account_file(
        path_to_service_account_json
    )
    scoped_credentials = creds.with_scopes(api_scopes)

    discovery_url = "{}?version={}".format(discovery_api, api_version)

    return discovery.build(
        service_name,
        api_version,
        discoveryServiceUrl=discovery_url,
        credentials=scoped_credentials,
    )


# Get an IoT client using the GCP project (NOT firebase proj!)
iot_client = get_IoT_client(env_vars.path_to_google_service_account)


# ------------------------------------------------------------------------------
# Get the count of IoT registrations.
def get_iot_registrations():

    # path to the device registry
    registry_name = "projects/{}/locations/{}/registries/{}".format(
        env_vars.cloud_project_id, env_vars.cloud_region, env_vars.device_registry
    )

    try:
        # get devices registry and list
        devices = iot_client.projects().locations().registries().devices()
        list_of_devices = devices.list(parent=registry_name).execute().get(
            "devices", []
        )
    except errors.HttpError as e:
        print("get_iot_registrations: ERROR: " "HttpError: {}".format(e._get_reason()))
        return False

    res = {}
    res["registered"] = "{:,}".format(len(list_of_devices))
    res["timestamp"] = dt.datetime.utcnow().strftime("%FT%XZ")
    return res


# Return a dict with a list of IoT devices with heartbeat and metadata.
def get_iot_device_list():

    # path to the device registry
    registry_name = "projects/{}/locations/{}/registries/{}".format(
        env_vars.cloud_project_id, env_vars.cloud_region, env_vars.device_registry
    )

    try:
        # get devices registry and list
        devices = iot_client.projects().locations().registries().devices()
        list_of_devices = devices.list(parent=registry_name).execute().get(
            "devices", []
        )
    except errors.HttpError as e:
        print("get_iot_device_list: ERROR: " "HttpError: {}".format(e._get_reason()))
        return False

    res = {}
    res["devices"] = []  # list of devices
    for d in list_of_devices:
        device_id = d.get("id")
        device_name = "{}/devices/{}".format(registry_name, device_id)
        device = devices.get(name=device_name).execute()
        last_heartbeat_time = device.get("lastHeartbeatTime", "")
        last_config_send_time = device.get("lastConfigSendTime", "Never")
        last_error_time = device.get("lastErrorTime", "")
        last_error_message = device.get("lastErrorStatus", {})
        last_error_message = last_error_message.get("message", "")
        metadata = device.get("metadata", {})
        user_uuid = metadata.get("user_uuid", None)
        device_notes = metadata.get("device_notes", "")
        device_name = metadata.get("device_name", "")

        dev = {}
        dev["device_uuid"] = device_id  # MUST use key 'device_uuid' to match DS
        dev["last_heartbeat_time"] = last_heartbeat_time
        dev["last_error_time"] = last_error_time
        dev["last_error_message"] = last_error_message
        dev["last_config_send_time"] = last_config_send_time  # last recipe sent
        dev["user_uuid"] = user_uuid
        dev["device_notes"] = device_notes
        dev["device_name"] = device_name

        res["devices"].append(dev)

    res["timestamp"] = dt.datetime.utcnow().strftime("%FT%XZ")
    return res

# Delete a device, returns result from google API.
def delete_iot_device(device_id):

    # path to the device registry & device
    registry_name = "projects/{}/locations/{}/registries/{}".format(
        env_vars.cloud_project_id, env_vars.cloud_region, env_vars.device_registry
    )
    device_name = "{}/devices/{}".format(registry_name, device_id)

    try:
        # get devices registry
        devices = iot_client.projects().locations().registries().devices()
        devices.delete(name=device_name).execute()
        return True
    except errors.HttpError as e:
        print("delete_iot_device: ERROR: " "HttpError: {}".format(e._get_reason()))
    return False


def send_recipe_to_device_via_IoT(device_id, commands_list):
    # get the latest config version number (int) for this device
    device_path = "projects/{}/locations/{}/registries/{}/devices/{}".format(
        env_vars.cloud_project_id,
        env_vars.cloud_region,
        env_vars.device_registry,
        device_id,
    )
    devices = iot_client.projects().locations().registries().devices()
    configs = devices.configVersions().list(name=device_path).execute().get(
        "deviceConfigs", []
    )

    latestVersion = 1  # the first / default version
    if 0 < len(configs):
        latestVersion = configs[0].get("version")
        # print('send_recipe_to_device_via_IoT: Current config version: {}' \
        #    'Received on: {}\n'.format( latestVersion,
        #        configs[0].get('cloudUpdateTime')))

    # JSON commands array we send to the device
    # {
    #    "messageId": "<messageId>",   # number of seconds since epoch
    #    "deviceId": "<deviceId>",
    #    "commands": [
    #        {
    #            "command": "<command>",
    #            "arg0": "<arg0>",
    #            "arg1": "<arg1>"
    #        },
    #        {
    #            "command": "<command>",
    #            "arg0": "<arg0>",
    #            "arg1": "<arg1>"
    #        }
    #    ]
    # }

    # can only update the LATEST version!  (so get it first)
    version = latestVersion

    # send a config message to a device
    config = {}  # a python dict
    config["lastConfigVersion"] = str(version)
    config["messageId"] = str(int(time.time()))  # epoch seconds as message ID
    config["deviceId"] = str(device_id)
    config["commands"] = commands_list

    config_json = json.dumps(config)  # dict to JSON string
    print(
        "send_recipe_to_device_via_IoT: Sending commands to device: {}".format(
            config_json
        )
    )

    config_body = {
        "versionToUpdate": version,
        "binaryData": base64.urlsafe_b64encode(config_json.encode("utf-8")).decode(
            "ascii"
        ),
    }
    res = iot_client.projects().locations().registries().devices().modifyCloudToDeviceConfig(
        name=device_path, body=config_body
    ).execute()
    # print('config update result: {}'.format( res ))


def send_start_recipe_command(device_uuid, recipe_uuid, recipe_dict=None):
    print("Sending start recipe command to device")

    # Initialize device path
    device_path = "projects/{}/locations/{}/registries/{}/devices/{}".format(
        env_vars.cloud_project_id,
        env_vars.cloud_region,
        env_vars.device_registry,
        device_uuid,
    )

    # Initialize command
    command = {"recipe_uuid": recipe_uuid}

    # Check if sending full recipe
    # NOTE: Future designs should not send entire recipe blobs, just the recipe uuid
    if recipe_dict != None:
        command["recipe_dict"] = recipe_dict

    # Convert command dict to json
    command_json = json.dumps(command)

    # Initialize command body
    command_body = {
        "subfolder": "recipe/start",
        "binaryData": base64.urlsafe_b64encode(command_json.encode("utf-8")).decode(
            "ascii"
        ),
    }

    # Send command to device
    try:
        iot_client.projects().locations().registries().devices().sendCommandToDevice(
            name=device_path, body=command_body
        ).execute()
    except errors.HttpError as e:
        content = json.loads(e.content)
        error = content.get("error", {})
        message = error.get("message")
        raise SendCommandError(message)

# Create an entry in the Google IoT device registry.
# This is part of the device registration process that allows it to communicate
# with the backend.
# Returns: device_ID, device_software_version
def create_iot_device_registry_entry(
    verification_code, device_name, device_notes, device_type, user_uuid
):
    # get a firestore DB collection of the RSA public keys uploaded by
    # a setup script on the device:
    keys_ref = fs_client.collection(u"devicePublicKeys")

    # docs = keys_ref.get()  # get all docs
    # for doc in docs:
    #    key_id = doc.id
    #    keyd = doc.to_dict()
    #    print(u'doc.id={}, doc={}'.format( key_id, keyd ))
    #    key = keyd['key']
    #    cksum = keyd['cksum']
    #    state = keyd['state']
    #    print('key={}, cksum={}, state={}'.format(key,cksum,state))

    # query the collection for the users code
    query = keys_ref.where(u"cksum", u"==", verification_code)
    docs = list(query.get())
    if not docs:
        print(
            "create_iot_device_registry_entry: ERROR: "
            'Verification code "{}" not found.'.format(verification_code)
        )
        raise ValueError('Verification code "{}" not found.'.format(verification_code))

    # get the single matching doc
    doc = docs[0]
    key_dict = doc.to_dict()
    doc_id = doc.id

    # verify all the keys we need are in the doc's dict
    for key in ["key", "cksum", "state", "MAC"]:
        if key not in key_dict:
            print(
                "create_iot_device_registry_entry: ERROR: "
                "Missing {} in {}".format(key, key_dict)
            )
            raise ValueError(
                "Device not registered properly." " Please register again."
            )

    public_key = key_dict.get("key")
    cksum = key_dict.get("cksum")
    state = key_dict.get("state")
    MAC = key_dict.get("MAC")
    version = key_dict.get("version", None)  # only on newer clients

    # print( 'doc_id={}, cksum={}, state={}, MAC={}'.format(
    #        doc_id, cksum, state, MAC ))
    # print('public_key:\n{}'.format( public_key ))

    # Generate a unique device id from code + MAC.
    # ID MUST start with a letter!
    # (test ID format in the IoT core console)
    # Start and end your ID with a lowercase letter or a number.
    # You can also include the following characters: + . % - _ ~
    device_id = "{}-{}-{}".format(device_type, verification_code, MAC)

    # register this device using its public key we got from the DB
    device_template = {
        "id": device_id,
        "credentials": [{"publicKey": {"format": "RSA_X509_PEM", "key": public_key}}],
        "metadata": {
            "user_uuid": user_uuid,
            "device_name": device_name,
            "device_notes": device_notes,
        },
    }

    # path to the device registry
    registry_name = "projects/{}/locations/{}/registries/{}".format(
        env_vars.cloud_project_id, env_vars.cloud_region, env_vars.device_registry
    )

    try:
        # add the device to the IoT registry
        devices = iot_client.projects().locations().registries().devices()
        devices.create(parent=registry_name, body=device_template).execute()
    except errors.HttpError as e:
        print(
            "create_iot_device_registry_entry: ERROR: "
            "HttpError: {}".format(e._get_reason())
        )
        raise

    print(
        "create_iot_device_registry_entry: "
        "Device {} added to the {} registry.".format(
            device_id, env_vars.device_registry
        )
    )

    # mark device state as verified
    # (can only call update on a DocumentReference)
    doc_ref = doc.reference
    doc_ref.update({u"state": u"verified"})

    return device_id, version  # put this id in the datastore of user's devices
