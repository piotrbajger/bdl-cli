import os
import time
import requests

import pandas as pd


VARIABLE_IDS = ["744856", "744857"]
UNIT_IDS = ["012415075011"]


MAX_VAR_NAME_LEVEL = 10
BASE_URL = "https://bdl.stat.gov.pl/api/v1"
HEADERS = {
    "X-ClientId": os.getenv("BDL_API_KEY"),
}


def _get_json_response(url, params):
    response = requests.get(url, params=params, headers=HEADERS)
    time.sleep(0.5)
    response.raise_for_status()
    return response.json()


def get_unit_data(unit_id, variable_ids):
    url = f"{BASE_URL}/data/by-unit/{unit_id}"
    params = {
        "format": "json",
        "var-id": variable_ids,
    }
    resp = _get_json_response(url, params)
    return json_to_dataframe(resp)


def get_variables():
    def _parse_variable(var_dict):
        name_parts = []
        for i in range(1, MAX_VAR_NAME_LEVEL):
            name_part = var_dict.get(f"n{i}")
            if name_part is None:
                break
            name_parts.append(name_part)
        if not len(name_parts):
            raise RuntimeError(f"No name levels in {var_dict}")
        parsed_dict = {
            "variable_id": var_dict["id"],
            "variable_name": " - ".join(name_parts),
            "variable_unit": var_dict["measureUnitName"],
        }
        return parsed_dict

    variables = []
    for var_id in VARIABLE_IDS:
        url = f"{BASE_URL}/variables/{var_id}"
        params = {"format": "json", "lang": "pl"}
        var_json = _get_json_response(url, params)
        variables.append(_parse_variable(var_json))
    df = pd.DataFrame(variables)
    df.to_csv("data/variables.csv", index=False, encoding="utf-8")
    return df


def json_to_dataframe(json_data):
    records = []

    unit_id = json_data["unitId"]
    unit_name = json_data["unitName"]

    for result in json_data["results"]:
        variable_id = result["id"]
        for value in result["values"]:
            records.append(
                {
                    "unit_id": unit_id,
                    "unit_name": unit_name,
                    "variable_id": variable_id,
                    "year": value["year"],
                    "value": value["val"],
                }
            )

    return pd.DataFrame(records)


if __name__ == "__main__":
    variables = get_variables()
    variable_ids = variables["variable_id"].to_list()
    all_data = []
    for unit_id in UNIT_IDS:
        unit_data = get_unit_data(
            unit_id="012415075011",
            variable_ids=variable_ids,
        )

    df = unit_data.merge(variables, how="left", on="variable_id")
    df_wide = df.pivot(
        index=["unit_id", "variable_id", "unit_name", "variable_name", "variable_unit"],
        columns="year",
        values="value",
    )
    df_wide = df_wide.reset_index()
    df_wide.to_csv("data/all_data.csv", index=False, encoding="utf-8")
    print(df_wide)
