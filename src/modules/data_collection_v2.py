import requests
import pandas as pd
from browser_cookie3 import chrome
from datetime import date

COLUMNS = [
    "stp_indicator",
    "transport_type",
    "schedule_uid",
    "run_date",
    "train_identity",
    "this_tiploc",
    "this_crs",
    "origin_tiploc",
    "origin_description",
    "destination_tiploc",
    "destination_description",
    "gbtt_arr",
    "gbtt_dep",
    "wtt_arr",
    "wtt_dep",
    "wtt_pass",
    "actual_arr",
    "actual_arr_delay_mins",
    "actual_dep",
    "actual_dep_delay_mins",
    "actual_pass",
    "actual_pass_delay_mins",
    "platform",
    "platform_actual",
    "lead_class",
    "num_vehicles",
]

RUN_DATE = date(2024, 6, 1)   # change this
CRS = "RDG"
OUTFILE = f"RDG_{RUN_DATE}.csv"


def mins(t):
    if not t:
        return None
    return int(t[:2]) * 60 + int(t[2:])


def delay(actual, planned):
    if not actual or not planned:
        return None
    return mins(actual) - mins(planned)


def extract_csv_row(svc, run_date):
    ld = svc.get("locationDetail", {})

    return {
        "stp_indicator": svc.get("stpIndicator"),
        "transport_type": "T",
        "schedule_uid": svc.get("uid"),
        "run_date": run_date.isoformat(),
        "train_identity": svc.get("trainIdentity"),
        "this_tiploc": ld.get("tiploc"),
        "this_crs": CRS,
        "origin_tiploc": svc["origin"][0]["tiploc"],
        "origin_description": svc["origin"][0]["description"],
        "destination_tiploc": svc["destination"][-1]["tiploc"],
        "destination_description": svc["destination"][-1]["description"],
        "gbtt_arr": ld.get("gbttBookedArrival"),
        "gbtt_dep": ld.get("gbttBookedDeparture"),
        "wtt_arr": ld.get("publicArrival"),
        "wtt_dep": ld.get("publicDeparture"),
        "wtt_pass": ld.get("publicPass"),
        "actual_arr": ld.get("realtimeArrival"),
        "actual_arr_delay_mins": delay(
            ld.get("realtimeArrival"),
            ld.get("gbttBookedArrival")
        ),
        "actual_dep": ld.get("realtimeDeparture"),
        "actual_dep_delay_mins": delay(
            ld.get("realtimeDeparture"),
            ld.get("gbttBookedDeparture")
        ),
        "actual_pass": ld.get("realtimePass"),
        "actual_pass_delay_mins": delay(
            ld.get("realtimePass"),
            ld.get("publicPass")
        ),
        "platform": ld.get("platform"),
        "platform_actual": ld.get("platform"),
        "lead_class": svc.get("leadClass"),
        "num_vehicles": svc.get("vehicleCount"),
    }
