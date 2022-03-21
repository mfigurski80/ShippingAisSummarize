from math import radians, cos, sin, asin, sqrt
import os
import csv
from datetime import datetime

RADIUS = 6371  # of earth, in km


def distance(lat1, lon1, lat2, lon2):
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    lon1 = radians(lon1)
    lon2 = radians(lon2)

    # Haversine Formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return c * RADIUS


def readDT(t_str):
    return datetime.strptime(t_str, "%Y-%m-%dT%H:%M:%S")


def initCSV(filename, columns) -> bool:
    if os.path.exists(filename) and os.stat(filename).st_size != 0:
        return False
    f = open(filename, "w")
    wr = csv.writer(f)
    wr.writerow(columns)
    f.close()
    return True


def readShipDataset(filename) -> dict:
    f = open(filename)
    r = csv.reader(f)
    ships = {}
    next(r)  # skip header
    for row in r:
        ships[int(row[0])] = row[1:]
    f.close()
    return ships
