import csv
from datetime import datetime

from utils import readDT

from shipDaily import shipDailyDataIter


def dailyDataIterator(r):
    """Iterator that goes over dataset and yields
    a list of points for each day
    """


def summarizeDaily(in_r, out_wr):
    pass


def performSummarizeDaily(in_fname, out_fname):
    in_f = open(in_fname)
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
    summarizeDaily(csv.reader(in_f), csv.writer(out_f))
    in_f.close()
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
    performSummarizeDaily("./tmp.csv", "DAILY.csv")
