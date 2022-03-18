import csv
import os
from datetime import datetime

from distance import distance


def initCSV(filename, columns) -> bool:
    if os.path.exists(filename) and os.stat(filename).st_size != 0:
        return False
    f = open(filename, "w")
    wr = csv.writer(f)
    wr.writerow(columns)
    f.close()
    return True


def shipDailyDataIter(r):
    """Iterator that goes over full dataset and
    yields (shipId, ship, day), where ship is list of points
    and day is datetime.date
    Assumes full dataset is segmented by *day* but not
    by *shipId*, meaning that once the day changes, it
    will never repeat
    """
    cur_date = datetime(2000, 1, 1).date()
    ships_dict = {}
    for row in r:
        new_date = datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%S")
        if cur_date != new_date.date():  # new date!
            print(f"Found new date? {cur_date} -- {len(ships_dict)} ships")
            cur_date = new_date.date()
            if ships_dict == {}:
                continue
            for k in ships_dict:
                yield (k, ships_dict[k], cur_date)
            ships_dict = {}

        ship_id = int(row[0])
        if ship_id not in ships_dict:
            ships_dict[ship_id] = [row]
        else:
            ships_dict[ship_id].append(row)


def summarize(r, wr):
    cur_date = None
    # skip header row
    next(r)
    for (ship, data, day) in shipDailyDataIter(r):
        print(ship)
    pass


if __name__ == "__main__":
    agg_f = open("../results/AGGREGATE_AIS.csv")
    summ_f = open("./SUMMARIZED_DAILY_AIS.csv", "w")
    summarize(csv.reader(agg_f), csv.writer(summ_f))
    agg_f.close()
    ship_f.close()
