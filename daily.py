import csv
from datetime import datetime

from utils import readShipDataset

from shipDaily import shipDailyDataIter, summarizeShipDayData


def dailyDataIterator(r, ship_data):
    """Iterator that goes over dataset and yields
    a list of *daily* points for each day and a
    date: ([], datetime)
    """
    for (ships, day) in shipDailyDataIter(r):
        res = [
            summarizeShipDayData(ship, ships[ship], day, ship_data) for ship in ships
        ]
        yield (list(filter(None, res)), day)


def summarizeDaily(in_r, out_wr, ship_data):
    # print(ship_data.keys())
    # skip header...
    next(in_r)
    for (data, day) in dailyDataIterator(in_r, ship_data):
        continue
    pass


def performSummarizeDaily(
    aggregate_fnames: [str], ship_fname: str, out_fname: str = "./DAILY_AIS.csv"
):
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
                "totalShipArea",
            ]
        )
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
        ["../results/AGG-2016-2018-AIS.csv", "../results/AGGREGATE_AIS.csv"],
        "../filterData/ships.csv",
    )
