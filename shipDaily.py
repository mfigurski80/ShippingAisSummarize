import csv

from datetime import datetime, timedelta
import functools

from utils import distance, readDT, initCSV, readShipDataset


def shipDailyDataIter(r):
    """Iterator that goes over full dataset and
    yields (shipId, ship, day), where ship is
    list of points and day is datetime.date
    Assumes full dataset is segmented by *day*
    but not by *shipId*, meaning that once the
    day changes, it will never repeat
    """
    cur_date = datetime(2000, 1, 1).date()
    ships_dict = {}
    for row in r:
        new_date = readDT(row[1]).date()
        if cur_date != new_date:  # new date!
            print(f"Date: {cur_date} -- {len(ships_dict)} ships")
            if ships_dict == {}:
                cur_date = new_date
                continue
            yield (ships_dict, cur_date)
            cur_date = new_date
            ships_dict = {}

        ship_id = int(row[0])
        if ship_id not in ships_dict:
            ships_dict[ship_id] = [row]
        else:
            ships_dict[ship_id].append(row)


def getChangeSummation(d, measure):
    return round(sum([measure(d[i - 1], d[i]) for i in range(1, len(d))]), 2)


def getDistanceTraveled(d):
    return getChangeSummation(
        d, lambda a, b: distance(float(a[2]), float(a[3]), float(b[2]), float(b[3]))
    )


def getCargoChange(d):
    return getChangeSummation(d, lambda a, b: abs(float(b[7]) - float(a[7])))


def summarizeShipDayData(ship, data, day, ship_data):
    # still need kmCovered, startCargo, cargoDiff
    # print(f"Ship {ship} -- {len(data)} entries on {day}")
    if ship not in ship_data or int(ship_data[ship][1]) >= 90:
        return None
    data.sort(key=lambda r: r[1])
    # print(f"First Entry:", data[0])
    # print(f"Last Entry:", data[-1])
    chCargo = None
    try:
        chCargo = getCargoChange(data)
    except:
        return None
    return [
        ship,
        day,
        data[0][8],
        data[0][2],
        data[0][3],
        getDistanceTraveled(data),
        data[0][7],
        chCargo,
        readDT(data[-1][1]).time(),
    ]


def summarizeShipDaily(r, wr, ship_data):
    # skip header row
    next(r)

    for (ships, day) in shipDailyDataIter(r):
        for ship in ships:
            summary = summarizeShipDayData(ship, ships[ship], day, ship_data)
            if summary is None:
                continue
            wr.writerow(summary)


def performSummarizeShipDaily(
    aggregate_fnames: [str], ship_fname: str, out_fname: str = "./SHIP_DAILY_AIS.csv"
):
    initCSV(
        out_fname,
        [
            "MMSI",
            "date",
            "portId",
            "startLong",
            "startLat",
            "kmCovered",
            "cargo",
            "chCargo",
            "lastTime",
        ],
    )
    summ_f = open(out_fname, "a")
    ship_data = readShipDataset(ship_fname)
    for fname in aggregate_fnames:
        agg_f = open(fname)
        summarizeShipDaily(csv.reader(agg_f), csv.writer(summ_f), ship_data)
        agg_f.close()
    summ_f.close()


if __name__ == "__main__":
    performSummarizeShipDaily(
        ["../results/AGG-2016-2018-AIS.csv", "../results/AGGREGATE_AIS.csv"],
        "../filterData/ships.csv",
    )
