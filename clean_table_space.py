#!/usr/bin/python3
"""
This script is for cleaning kamal-123 project in BigQuery after processing manually de data of CDO
"""
from __future__ import absolute_import

import sys
import argparse
import os
from datetime import date, timedelta


parser = argparse.ArgumentParser(description='Borrar datos bq')
parser.add_argument( "--start", dest="start", required=True, type=str, help="Date Start")
parser.add_argument( "--end", dest="end", required=True, type=str, help="Date End")

def raw():
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    
    if args.end:
        end_date = date.fromisoformat(args.end)

    if end_date < start_date:
        print(f"The start-date must be greater than the end-date: {args.start, args.end}")
        return

    # Getting date range between start and end
    delta = end_date - start_date
    for i in range(delta.days + 1):
        day = (start_date + timedelta(days=i)).strftime("%Y%m%d")
        print(f"DROP TABLE kamal-123.datalake_00_raw - {day}")
        os.system(f"bq query --nouse_legacy_sql DROP TABLE kamal-123.datalake_00_raw.actix_one_3g_{day}")
        os.system(f"bq query --nouse_legacy_sql DROP TABLE kamal-123.datalake_00_raw.actix_one_4g_{day}")

def clean():
    print("TRUNCATE  kamal-123.datalake_01_clean.actix_one")
    os.system("bq query --nouse_legacy_sql DELETE FROM kamal-123.datalake_01_clean.actix_one_3g WHERE start_date BETWEEN '{start_date}' AND '{end_date}'")
    os.system("bq query --nouse_legacy_sql DELETE FROM kamal-123.datalake_01_clean.actix_one_4g WHERE start_date BETWEEN '{start_date}' AND '{end_date}'")


def mobility_trace():
    print("TRUNCATE kamal-123.datalake_02_entity.mobility_trace")
    os.system("bq query --nouse_legacy_sql DELETE FROM kamal-123.datalake_02_entity.mobility_trace WHERE start_date BETWEEN '{start_date}' AND '{end_date}'")


if __name__ == "__main__":
    raw()
    clean()
    mobility_trace()


# pyhon clean.py --start=2021-05-09  --end=2021-04-11