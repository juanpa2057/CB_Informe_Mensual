import requests
import pandas as pd
import json
import time
import datetime as dt


import os
from dotenv import load_dotenv
load_dotenv()

_TOKEN: str = os.environ["_token"]

class Configuration:
    LST_VAR_FIELDS = ["value.value", "variable.id",
                  "device.label", "device.name", "timestamp"]
    LST_HEADERS = ['value', 'variable', 'device', 'device_name', 'timestamp']

    DATE_FORMAT = "%Y-%m-%d"
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    LOCAL_TIMEZONE = 'America/Bogota'


class Ubidots:
    def sendDatatoUbidots(pload, headers, request):
        r = requests.post(request, headers=headers, json=pload)
        if 200 <= r.status_code <= 299:
            print("Sent", r.text, "with response code: ", r.status_code)
        if 400 <= r.status_code <= 499:
            print("Retrying...", r.text)
            time.sleep(5)
        time.sleep(1)
        return r.text

    def makeUbidotsPayload(value, timestamp, timestampformat):
        pload = {"value": value,
                 "timestamp": str(int(dt.datetime.timestamp(dt.datetime.strptime(timestamp, timestampformat))))+'000'}
        return pload

    def makeUbidotsRequest(device_id, variable_id):
        request = 'https://industrial.api.ubidots.com/api/v1.6/devices/' + \
            device_id+'/'+variable_id+'/values'+'/?force=true'
        return request

    def makeUbidotsHeaders(TOKEN):
        headers = {'X-Auth-Token': TOKEN, 'Content-Type': 'application/json'}
        return headers

    def Download_from_ubidots(device_label, variable_label, datarange, timestamp_format, TOKEN):
        try:
            datarange_object = {'start': int(dt.datetime.timestamp(dt.datetime.strptime(datarange['start'] + 'T00:00:00', timestamp_format))), 'end': int(
                dt.datetime.timestamp(dt.datetime.strptime(datarange['end'] + 'T00:00:00', timestamp_format)))}
            pload = {'token': TOKEN}
            r = requests.get('https://industrial.api.ubidots.com/api/v1.6/devices/' + device_label + '/' + variable_label +
                             '/values?page_size=1?start=' + str(datarange_object['start'])+'000'+'&end='+str(datarange_object['end']) + '000', params=pload)
            df = pd.json_normalize(r.json(), record_path=['results'])
            timestamps = df["timestamp"] = pd.to_datetime(
                df["timestamp"], unit='ms')
            df.set_index('timestamp', inplace=True)
            df["created_at"] = pd.to_datetime(df["created_at"], unit='ms')
            df.drop(['created_at'], axis=1, inplace=True)
            df = df.reindex(index=df.index[::-1])
        except:
            pass
        return df.rename(columns={"value": variable_label})

    def get_device_group_devices(token, device_group_label):
        pload = {'token': token}
        r = requests.get(
            'https://industrial.api.ubidots.com/api/v2.0/device_groups/' +
            device_group_label+'/devices/?token='+token, params=pload
        )

        JSON = r.json()

        devices = {
            "device_name": [],
            "id": [],
            "label": []
        }
        for JSON_item in JSON['results']:
            devices["device_name"].append(JSON_item['name'])
            devices["id"].append(JSON_item['id'])
            devices["label"].append(JSON_item['label'])

        return devices

    def get_concatenated_dataframe_multiple_devices(df, device_group_devices, variable_label, datarange, timestamp_format, token):
        for device_label in device_group_devices["label"]:
            req_data = Ubidots.Download_from_ubidots(
                device_label, variable_label, datarange, timestamp_format, token)
            df = df.merge(req_data, left_on='timestamp',
                          right_on='timestamp', how='left')
        return df

    def get_all_variables_from_device(token, device_key):
        pload = {'token': token}
        r = requests.get('https://industrial.api.ubidots.com/api/v2.0/devices/' +
                         device_key+'/variables/?token='+token, params=pload)
        r.text
        JSON = r.json()

        if not 'results' in JSON or len(JSON['results']) == 0:
            return dict()

        variables = {
            "variable_name": [],
            "variable_id": [],
            "variable_label": []
        }
        for JSON_item in JSON['results']:
            variables["variable_name"].append(JSON_item['name'])
            variables["variable_id"].append(JSON_item['id'])
            variables["variable_label"].append(JSON_item['label'])
        return variables

    def get_concatenated_dataframe_from_device(variables, device_label, datarange, variables_to_download, timestamp_format, token):
        df = pd.DataFrame()
        for variable_label in variables["variable_label"]:
            if variable_label in variables_to_download:
                req_data = Ubidots.Download_from_ubidots(
                    device_label,
                    variable_label,
                    datarange,
                    timestamp_format,
                    token
                )

                print(f"{device_label} / {variable_label} / size: {req_data.shape}")
                df = df.merge(req_data, left_on='timestamp',
                              right_on='timestamp', how='left')
        return df

    def get_raw_data(lst_var_id, lst_var_fields, start_timestamp, end_timestamp, token, join=False):
        req_url = "https://industrial.api.ubidots.com/api/v1.6/data/raw/series"

        headers_list = {
            "Accept": "*/*",
            "X-Auth-Token": token,
            "Content-Type": "application/json"
        }

        # lst_var_id must be passed as a list
        if not isinstance(lst_var_id, list):
            lst_var_id = list(lst_var_id)

        payload = json.dumps({
            "variables": lst_var_id,
            "columns": lst_var_fields,
            "join_dataframes": join,
            "start": start_timestamp,
            "end": end_timestamp
        })

        return requests.request("POST", req_url, data=payload,  headers=headers_list)

    def get_var_id_for_multiple_devices(lst_devices, token):
        lst_var_id = []
        lst_var_label = []
        lst_rows = []
        for device_id in lst_devices:
            response = Ubidots.get_all_variables_from_device(token, device_id)
            lst_var_id.extend(response['variable_id'])
            lst_var_label.extend(response['variable_label'])

            for idx in range(len(response['variable_id'])):
                lst_rows.append(
                    [
                        response['variable_id'][idx],
                        response['variable_label'][idx],
                        device_id
                    ]
                )

            # print(lst_var_label)
            # print("-"*79)
        df = pd.DataFrame(data=lst_rows, columns=[
                          'variable_id', 'variable_label', 'device_id'])

        return df

    def get_gps_for_multiple_device_id(lst_device_ids, token=_TOKEN):
        coordinates = {
            "device_name": [],
            "latitude": [],
            "longitude": [],
            # "value":[]
        }

        for device in lst_device_ids:
            pload = {'token': token}
            r = requests.get(
                'https://industrial.api.ubidots.com/api/v2.0/devices/'
                + str(device) + '/?token='+token, params=pload
            )

            JSON = r.json()

            coords = {"lat": None, "lng": None}
            if "_location_fixed" in JSON["properties"]:
                coords = JSON["properties"]["_location_fixed"]

            coordinates["latitude"].append(coords["lat"])
            coordinates["longitude"].append(coords["lng"])
            coordinates["device_name"].append(JSON["name"])
            # coordinates["value"].append(float(front_month[front_month["device_name"]==JSON["name"]]["value"].values))

        return pd.DataFrame(data=coordinates)

    def ceildiv(a, b):
        return -(a // -b)

    

    def get_available_variables(device_id):
        if not isinstance(device_id, list):
            if isinstance(device_id, str):
                device_id = [device_id]
            else:
                device_id = list(device_id)

        return Ubidots.get_var_id_for_multiple_devices(device_id, _TOKEN)

    def make_request(VAR_IDS_TO_REQUEST, date_interval):
        # the request must be made in millisecond timestamps
        start_timestamp = Ubidots.str_date_to_int_timestamp_ms(
            date_interval['start'], Configuration.DATE_FORMAT)
        end_timestamp = Ubidots.str_date_to_int_timestamp_ms(
            date_interval['end'], Configuration.DATE_FORMAT)

        # The request is made with an array of all variable IDs.
        response = Ubidots.get_raw_data(
            VAR_IDS_TO_REQUEST,
            Configuration.LST_VAR_FIELDS,
            start_timestamp,
            end_timestamp,
            _TOKEN,
            join=False
        )

        # The connection is left open by default
        response.close()
        return response

    def str_date_to_int_timestamp_ms(date_string, date_format):
        element = dt.datetime.strptime(date_string, date_format)
        return int(dt.datetime.timestamp(element)) * 1000

    def parse_response(lst_response, DCT_VAR_ID_TO_LABEL):
        lst_df = []
        for res in lst_response:
            df_temp = Ubidots.flatten_bulk_raw_response(
                res.json()['results'], Configuration.LST_HEADERS)
            df_temp = Ubidots.parse_flat_data(df_temp, DCT_VAR_ID_TO_LABEL)

            lst_df.append(df_temp)

        return pd.concat(lst_df)

    def flatten_bulk_raw_response(r_json_data, headers):
        lst_df_idx = []
        for idx in range(len(r_json_data)):
            df_idx = pd.DataFrame(r_json_data[idx], columns=headers)
            lst_df_idx.append(df_idx)

        return pd.concat(lst_df_idx).reset_index(drop=True)

    def parse_flat_data(df, DCT_VAR_ID_TO_LABEL):
        # The Ubidots API does not return a variable-label field
        # and naming is inconsistent, so labels must be mapped from ids.
        df['variable'] = df['variable'].map(DCT_VAR_ID_TO_LABEL)

        # datetimes are human readable
        df["datetime"] = pd.to_datetime(df["timestamp"], unit='ms')
        df = Ubidots.convert_timezone(df)

        df.drop_duplicates(
            subset=['timestamp', 'variable', 'device'], inplace=True)
        return df.drop(columns='timestamp')

    def convert_timezone(obj, from_tz='utc', to_tz='America/Bogota'):
        if isinstance(obj, str):
            obj = pd.to_datetime(obj).tz_localize(from_tz).tz_convert(to_tz)
        elif isinstance(obj, dt.datetime):
            obj.tz_localize(from_tz).tz_convert(to_tz)
        elif isinstance(obj, pd.DataFrame):
            # A DatetimeIndex must be set to allow for easy
            # timezone conversion
            obj.set_index('datetime', inplace=True)
            obj = obj.tz_localize(from_tz).tz_convert(to_tz)

        return obj


    def get_available_devices_v2(label, level, page_size=100, token=_TOKEN):
        # v2 will skip interim functions that add the tilde to the label
        # and also verify that dtype of the inputs. This should all be
        # handled within the functions. Maybe even consider making one
        # big request function and using pattern matching to select the
        # api url.
        """
        Level can take the values: 'group', 'organization', 'account'.
        """

        pload = {'token': token}

        if (level == 'group'):
            r = requests.get(
                'https://industrial.api.ubidots.com/api/v2.0/device_groups/~' +
                label+'/devices/?token='+token, params=pload
            )
        elif (level == 'organization'):
            r = requests.get(
                'https://industrial.api.ubidots.com/api/v2.0/organizations/~' +
                label+'/devices/?token=', params=pload
            )
        elif (level == 'account'):
            pload = {
                'token': token,
                'page_size':str(page_size)
            }

            r = requests.get(
                'https://industrial.api.ubidots.com/api/v2.0/devices/?token=',
                params=pload
            )
        else:
            #TODO: write this as a try except or something like that.
            print("Error: invalid level value")
            return None


        JSON = r.json()

        devices = {
            "device_name": [],
            "device_label": [],
            "device_id": []
        }
        for JSON_item in JSON['results']:
            devices["device_name"].append(JSON_item['name'])
            devices["device_label"].append(JSON_item['label'])
            devices["device_id"].append(JSON_item['id'])

        return pd.DataFrame(devices)
