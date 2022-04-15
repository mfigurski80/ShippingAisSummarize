import csv
from datetime import datetime
from calendar import monthrange

from utils import readShipDataset
from daily import getShipArea, dailyDataIterator as dailyShipDataIterator


def shipDailyDataIterByMonth(r, ship_data):
    daily_by_ship = {}
    cur_date = None
    month = -1
    for (data, date) in dailyShipDataIterator(r, ship_data):
        # check if have aggregated a monthly timeframe
        if date.month != month:
            if month == -1:
                month = date.month
                cur_date = date
            else:
                yield (daily_by_ship, cur_date)
                daily_by_ship = {}
                month = date.month
                cur_date = date
        # add data points
        for sh in data:
            MMSI = sh[0]
            if MMSI not in daily_by_ship:
                daily_by_ship[MMSI] = [sh]
            else:
                daily_by_ship[MMSI] += [sh]
    yield (daily_by_ship, cur_date)


def summarizeShipMonthData(data, ship_data, date):
    return (
        data[0][0],  # ship id
        date,  # month
        round(sum([d[5] for d in data]), 2),  # km covered
        len(data),  # days present
        len([d[5] for d in data if d[5] < 10]),  # days stationary
        getShipArea(data[0][0], ship_data),  # ship area
    )


def summarizeMonthData(data, date):
    (_, days_max) = monthrange(date.year, date.month)
    proportion_of_month_present = [d[3] / days_max for d in data]
    proportion_of_days_stationary = [d[4] / d[3] for d in data]
    return (
        date.strftime("%Y-%m"),  # month
        len(data),  # n ships
        round(sum([d[2] for d in data]), 2),  # km covered
        round(sum([d[3] for d in data]) / len(data), 2),  # avg days present
        round(sum([d[4] for d in data]) / len(data), 2),  # avg days stationary
        round(
            sum([d[4] / d[3] for d in data]) / len(data), 2
        ),  # avg proportion of stationary/days
        days_max,  # days in month
        sum([d[5] for d in data]),  # total ship area
        round(
            sum([d[5] * prop for (d, prop) in zip(data, proportion_of_month_present)]),
            2,
        ),  # total ship area present
        round(
            sum(
                [
                    d[5] * prop1 * prop2
                    for (d, prop1, prop2) in zip(
                        data, proportion_of_month_present, proportion_of_days_stationary
                    )
                ]
            ),
            2,
        ),  # proportion ship area present+stationary/days
        0,  # n ships entering
        0,  # n ships exiting
    )


def monthlyDataIter(r, ship_data):
    for (data, date) in shipDailyDataIterByMonth(r, ship_data):
        shipMonthData = [
            summarizeShipMonthData(data[sh], ship_data, date) for sh in data
        ]
        monthData = summarizeMonthData(shipMonthData, date)
        yield (monthData, date)


def summarizeMonthly(in_r, out_wr, ship_data):
    # skip header...
    next(in_r)
    for (data, date) in monthlyDataIter(in_r, ship_data):
        out_wr.writerow(data)
        print(f"{date} -- {data[1]} ships")


def performSummarizeMonthly(
    aggregate_fnames: [str], ship_fname: str, out_fname: str = "./MONTHLY_AIS.csv"
):
    print("Grouping data by month...")
    with open(out_fname, "w") as out_f:
        out_f.write(
            ",".join(
                [
                    "date",
                    "nShips",
                    "totalKmCovered",
                    "avgDaysPresent",
                    "avgDaysStationary",
                    "avgProportionOfStationaryDays",
                    "numDaysInMonth",
                    "totalShipArea",
                    "totalShipAreaPresent",
                    "totalShipAreaStationary",
                    "nShipsEntered",
                    "nShipsExited",
                ]
            )
            + "\n"
        )
        ship_data = readShipDataset(ship_fname)
        for fname in aggregate_fnames:
            with open(fname) as agg_f:
                summarizeMonthly(csv.reader(agg_f), csv.writer(out_f), ship_data)


if __name__ == "__main__":
    performSummarizeMonthly(
        [
            "../results/AGG-2015-2016-AIS.csv",
            "../results/AGG-2016-2018-AIS.csv",
            "../results/AGG_2018-2021.9-AIS.csv",
            "../results/AGG-2021.9-2022-AIS.csv",
        ],
        "../getFilterData/ships.csv",
    )
