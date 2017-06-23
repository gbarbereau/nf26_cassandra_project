import arrow
import math

def compute_from_ts(ts):
    date = arrow.Arrow.utcfromtimestamp(ts)
    return [date.year, date.month, date.week, date.day, date.isoweekday()]


def init_table(Bdd):
    date_attr = {
        "TRIP_ID": "text",
        "YEAR": "int",
        "MONTH": "int",
        "DAY": "int",
    }
    date_pk = ["(YEAR, MONTH, DAY)", "TRIP_ID"]

    type_day_attr = {
        "TRIP_ID": "text",
        "DAY_TYPE": "text"
    }
    type_day_pk = ["DAY_TYPE", "TRIP_ID"]
    call_attr = {
        "TRIP_ID": "text",
        "CALL_TYPE": "text",
        "ORIGIN_CALL": "text",
        "ORIGIN_STAND": "text",
    }
    call_pk = ["CALL_TYPE", "TRIP_ID"]
    taxi_trip_attr = {
        "TRIP_ID": "text",
        "TAXI_ID": "text"
    }
    taxi_trip_pk = ["TAXI_ID", "TRIP_ID"]
    traject_attr = {
        "TRIP_ID": "text",
        "TIMESTAMP": "text",
        "START": "tuple<float, float>",
        "END": "tuple<float, float>",
        "DIST": "float",
    }
    traject_pk = ["TRIP_ID"]
    Bdd.create_table(date_attr, date_pk, "DATE_COMPLETE")
    Bdd.create_table(type_day_attr, type_day_pk, "TYPE_DAY")
    Bdd.create_table(call_attr, call_pk, "CALL_TYPE")
    Bdd.create_table(taxi_trip_attr, taxi_trip_pk, "TAXI_TRIP")
    Bdd.create_table(traject_attr, traject_pk, "TRAJECT")

def compute_dist(dep, arr):
    lat1 = dep[0]
    lat2 = arr[0]
    lng1 = dep[1]
    lng2 = arr[1]
    dist = math.sqrt(((lat1 - lat2) / 360 * 2 * math.pi * 6371000) ** 2 + (
        (lng1 - lng2) / 360 * 2 * math.pi * 6371000 * math.cos((lat1 + lat2) / 360 * math.pi)) ** 2)
    return dist