import os
import time
import requests

import pandas as pd


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
        "year": "2023",
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
            "variable_subject_id": var_dict["subjectId"],
            "variable_name": " - ".join(name_parts),
            "variable_unit": var_dict["measureUnitName"],
        }
        return parsed_dict

    variables = []
    for var_id in get_variable_ids():
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


def get_unit_ids():
    with open("jednostki.txt") as f:
        unit_ids = f.readlines()
    unit_ids = [u.strip() for u in unit_ids if u.strip()]
    print(f"Ściągam dane dla {len(unit_ids)} jednostek terytorialnych.")
    return unit_ids


def get_variable_ids():
    with open("zmienne.txt") as f:
        var_ids = f.readlines()
    var_ids = [v.strip() for v in var_ids if v.strip()]
    print(f"Ściągam dane dla {len(var_ids)} zmiennych.")
    return var_ids


if __name__ == "__main__":
    unit_ids = get_unit_ids()
    variables = get_variables()
    variable_ids = variables["variable_id"].to_list()
    all_data = []
    for i, unit_id in enumerate(unit_ids, start=1):
        print(
            f"Ściągam dane dla {unit_id:>30s} ({i}/{len(unit_ids)})... ",
            end="",
        )
        unit_data = get_unit_data(
            unit_id=unit_id,
            variable_ids=variable_ids,
        )
        all_data.append(unit_data)
        print(f"{len(unit_data)} wierszy")

    df = pd.concat(all_data)
    df = df.merge(variables, how="left", on="variable_id")
    df["variable_name_and_unit"] = (
        "[" + df["variable_subject_id"] + "/"
        + df["variable_id"].astype(str) + "] "
        + df["variable_name"]
        + " [" + df["variable_unit"] + "]"
    )
    df_wide = df.pivot(
        index=["unit_name", "unit_id"],
        columns="variable_name_and_unit",
        values="value",
    )
    df_wide = df_wide.reset_index()

    output_path = "data/all_data.csv"
    df_wide.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Zapisuję {len(df_wide)} wierszy w {output_path}.")
