import csv
from datetime import datetime

from utils import readShipDataset

from shipDaily import shipDailyDataIter, summarizeShipDayData


def dailyDataIterator(r, ship_data):
    """Iterator that goes over dataset and yields
    a list of *ship day* points for a specific day and
    the associated date: ([[ShipDay]], datetime)
    """
    for (ships, day) in shipDailyDataIter(r):
        res = [
            summarizeShipDayData(ship, ships[ship], day, ship_data) for ship in ships
        ]
        # print(f"[daily.py dailyDataIterator] yielding {res}")
        yield (list(filter(None, res)), day)


def getShipArea(ship_id, ship_data):
    try:
        return float(ship_data[ship_id][2]) * float(ship_data[ship_id][3])
    except:
        print(f"FAILED SHIP DIMENSIONS: {ship_data[ship_id]}")
    try:
        return float(ship_data[ship_id][2]) * (0.12 * float(ship_data[ship_id][2]))
    except:
        pass
    return 0


def summarizeDaily(in_r, out_wr, ship_data):
    # print(ship_data.keys())
    # skip header...
    next(in_r)
    for (data, day) in dailyDataIterator(in_r, ship_data):
        out = [
            day,
            len(data),
            round(sum([ship[5] for ship in data]), 2),
            len([ship[5] for ship in data if float(ship[5]) < 10]),
            sum([float(ship[6]) for ship in data]),
            sum([float(ship[7]) for ship in data]),
            sum([float(ship[6]) for ship in data if float(ship[5]) < 10]),
            sum([getShipArea(ship[0], ship_data) for ship in data]),
            sum(
                [
                    getShipArea(ship[0], ship_data)
                    for ship in data
                    if float(ship[5]) < 10
                ]
            ),
        ]
        out_wr.writerow(out)
    pass


def performSummarizeDaily(
    aggregate_fnames: [str], ship_fname: str, out_fname: str = "./DAILY_AIS.csv"
):
    print("Grouping data by day...")
    out_f = open(out_fname, "w")
    out_f.write(
        ",".join(
            [
                "date",
                "nShips",
                "totalKmCovered",
                "nStationary",
                "totalCargo",
                "totalChCargo",
                "totalStationaryCargo",
                "totalShipArea",
                "totalStationaryArea",
            ]
        )
        + "\n"
    )
    ship_data = readShipDataset(ship_fname)
    for fname in aggregate_fnames:
        agg_f = open(fname)
        summarizeDaily(csv.reader(agg_f), csv.writer(out_f), ship_data)
        agg_f.close()
    out_f.close()


#  import pandas as pd
#  import matplotlib.pyplot as plt
#  d = pd.read_csv(fname)
#  print(d)
#  by_date = d[["MMSI", "date"]].groupby("date").count()
#  plt.plot(by_date.index, by_date["MMSI"])
#  plt.show()
#  m_traveled = d[["date", "kmCovered"]].groupby("date")


if __name__ == "__main__":
    performSummarizeDaily(
        [
            "../data/AGG-2015-2016-AIS.csv",
            "../data/AGG-2016-2018-AIS.csv",
            "../data/AGG_2018-2021.9-AIS.csv",
            "../data/AGG-2021.9-2022-AIS.csv",
        ],
        #  ["../data/AGG-2016-2018-AIS.csv"],
        "../getFilterData/ships.csv",
    )
