import csv
from tools import compute_from_ts
import ast
s = {
    "TRIP_ID": 0,
    "CALL_TYPE": 1,
    "ORIGIN_CALL": 2,
    "ORIGIN_STAND": 3,
    "TAXI_ID": 4,
    "TIMESTAMP": 5,
    "DAY_TYPE": 6,
    "MISSING_DATA": 7,
    "POLYLINE": 8
}

table_list = ['DATE_YEAR', 'DATE_MONTH', 'DATE_DAY', 'DATE_TYPE_DAY', 'CALL_TYPE', 'TAXI_TRIP', 'FACTS']

def open_file():
    csvfile = open("/home/e13/train.csv")
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    next(csvfile)
    return csv_reader


def global_insert(file, Bdd):
    count = 0
    global_count = 0
    list_query = []
    for elem in file:
        print(global_count)
        count += 1
        global_count += 1
        YEAR, MONTH, WEEK, DAY, WEEKDAY = compute_from_ts(elem[s["TIMESTAMP"]])
        TRIP_ID = elem[s["TRIP_ID"]]
        CALL_TYPE = elem[s["CALL_TYPE"]]
        ORIGIN_CALL = elem[s["ORIGIN_CALL"]]
        ORIGIN_STAND = elem[s["ORIGIN_STAND"]]
        TAXI_ID = elem[s["TAXI_ID"]]
        TIMESTAMP = elem[s["TIMESTAMP"]]
        DAY_TYPE = elem[s["DAY_TYPE"]]
        MISSING_DATA = elem[s["MISSING_DATA"]]
        POLYLINE = elem[s["POLYLINE"]]
        list_query.append(compute_polyline(Bdd, POLYLINE, MISSING_DATA, TRIP_ID, TIMESTAMP))
        list_query.append(Bdd.get_insert_query(["TRIP_ID", "YEAR", "DAY", "MONTH"], [TRIP_ID, YEAR, DAY, MONTH], 'DATE_COMPLETE'))
        list_query.append(Bdd.get_insert_query(["TRIP_ID", "DAY_TYPE"], [TRIP_ID, DAY_TYPE], 'DATE_TYPE_DAY'))
        list_query.append(Bdd.get_insert_query(["TRIP_ID", "CALL_TYPE", "ORIGIN_CALL", "ORIGIN_STAND"],
                                               [TRIP_ID, CALL_TYPE, ORIGIN_CALL, ORIGIN_STAND], 'CALL_TYPE'))
        list_query.append(Bdd.get_insert_query(["TRIP_ID", "TAXI_ID"], [TRIP_ID, TAXI_ID], 'TAXI_TRIP'))
        if count == 5000:
            Bdd.group_execute(list_query)
            list_query = []
            count = 0
    if count != 0:
        Bdd.group_execute(list_query)
    return global_count


def compute_polyline(Bdd, pol1, missing_data, trip_id, ts):
    if missing_data == "False":
        pol = [tuple(x) for x in ast.literal_eval(pol1)]
        if len(pol) > 0:
            depart = pol[0]
            arrivee = pol[len(pol) - 1]
            dist = 0
            for i in range(len(pol) - 1):
                dist += compute_dist(pol[i], pol[i + 1])
        else:
            depart = 0
            arrivee = 0
            dist = -1
    else:
        depart = 0
        arrivee = 0
        dist = -1
    return Bdd.get_insert_query(["TRIP_ID", "TIMESTAMP", "START", "END", "DIST"], [trip_id, ts, depart, arrivee, dist], "TRAJECT")


def file_insertion_handler(Bdd):
    file = open_file()
    nb_row = global_insert(file, Bdd)
    return nb_row