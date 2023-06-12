# from functools import lru_cache
import msoffcrypto, sqlite3, os
from datetime import datetime

import pandas as pd
import numpy as np
from db import contracts, costs, sched_data
import io
from dotenv import load_dotenv

load_dotenv()

SCHEDULE_DB = os.environ.get("SCHEDULE_DB", None)
assert SCHEDULE_DB is not None, "SCHEDULE_DB environment variable not set"

SCHEDULE_XLS = os.environ.get("SCHEDULE_XLS", None)
assert SCHEDULE_XLS is not None, "SCHEDULE_XLS environment variable not set"

SCHEDULE_TABLE = os.environ.get("SCHEDULE_TABLE", None)
assert SCHEDULE_TABLE is not None, "SCHEDULE_TABLE environment variable not set"

ENCRYPTION_PASSWORD = os.environ.get("ENCRYPTION_PASSWORD", None)
assert (
    ENCRYPTION_PASSWORD is not None
), "ENCRYPTION_PASSWORD environment variable not set"

workcenters = {x["part"]: x for x in list(sched_data.find({}, {"_id": 0}))}

if "7883R1" in workcenters:
    print(workcenters["7883R1"])

def convert_float_to_int(nbr: str) -> int:
    try:
        return int(nbr)
    except ValueError:
        nbr = nbr.replace(",", "").split(".")[0]
        return int(nbr)


def calculate_gross_profit(price: float, cost: float) -> float:
    try:
        return price - cost
    except TypeError:
        print(price, cost)
        return 0


def calculate_gross_margin(price: float, cost: float) -> float:
    try:
        return ((price - cost) / price) * 100
    except TypeError:
        print(price, cost)
        return 0
    except ZeroDivisionError:
        print(cost)
        return 0


def calculate_order_value(qty: float, price: float) -> float:
    try:
        return qty * price
    except TypeError:
        print(qty, price)
        return 0


def calculate_order_cost(qty: float, cost: float) -> float:
    try:
        return qty * cost
    except TypeError:
        print(qty, cost)
        return 0


def calculate_order_profit(value: float, cost: float) -> float:
    try:
        return value - cost
    except TypeError:
        print(value, cost)
        return 0


def get_workcenter(wc: str, item: str) -> str:
    global workcenters

    if item in workcenters:
        wc = workcenters[item].get("wc", "Not Found in SCHED.DATA2")

    return wc


def unencrypt_excel():
    with open(SCHEDULE_XLS, "rb") as f:
        file = io.BytesIO(f.read())

    decrypted = io.BytesIO()

    try:
        ms = msoffcrypto.OfficeFile(file)
        ms.load_key(password=ENCRYPTION_PASSWORD)
        ms.decrypt(decrypted)
    except Exception as e:
        # print(e)
        pass

    try:
        df = pd.read_excel(decrypted, "Schedule", header=1)
    except:
        df = pd.read_excel(file, "Schedule", header=1)

    df.columns = df.iloc[0]
    df.drop(df.index[0])

    df.columns = [
        "requested",
        "wh_issue_date",
        "pulled",
        "posted",
        "racks",
        "parts_prep",
        "ready",
        "wc_ready",
        "job_done",
        "request",
        "in_parts_prep_by",
        "l",
        "run_date_time",
        "n",
        "item",
        "wc",
        "tooling",
        "r",
        "description",
        "lot",
        "lot_info",
        "qty",
        "comments",
        "x",
        "mp",
        "pallets",
    ]

    df = df.loc[(df.lot != "") & (df.lot.notnull())][
        ["item", "lot", "run_date_time", "qty", "wc"]
    ].copy()

    df["wc"].fillna("", inplace=True)

    df = df.loc[~df["lot"].str.contains(r"[A-Z]")].copy()
    df["item"] = df["item"].astype(str)

    df["lot"] = df["lot"].astype(int)
    df["run_date_time"] = df["run_date_time"].astype(str)
    df["qty"] = df["qty"].apply(lambda x: convert_float_to_int(x))

    return df


def update(df_xls: pd.DataFrame):
    print("current:", len(df_xls))

    db = SCHEDULE_DB
    table = SCHEDULE_TABLE

    costs_from_db = list(costs.find({}))
    costs_map = {}
    for cost in costs_from_db:
        for item in cost["alias"]:
            costs_map[item] = cost["cost"]

    def get_cost_from_map(item: str, cost: float) -> float:
        if cost <= 0 or np.isnan(cost):
            if item in costs_map:
                return costs_map[item]
            return 0.00
        return cost

    date = datetime.now()
    last_month = date.month - 1
    beginning_of_period = datetime(date.year, last_month, 1)

    docs = list(
        contracts.find(
            {
                "contractname": {
                    "$nin": [
                        "HOSPITAL PRICE - 22-10-20",
                        "1-49 PRICE - 22-10-20",
                        "50+ PRICE - 22-10-20",
                    ]
                },
                "contractend": {"$gte": beginning_of_period},
            }
        )
    )
    prices_map = {}

    for doc in docs:
        for agreement in doc["pricingagreements"]:
            if agreement["item"] not in prices_map:
                prices_map[agreement["item"]] = []

            prices_map[agreement["item"]].append(agreement["price"])

    for item in prices_map:
        prices_map[item] = sum(prices_map[item]) / len(prices_map[item])

    def get_price_from_map(item: str, price: float) -> float:
        if price <= 0 or np.isnan(price):
            if item in prices_map:
                return prices_map[item]
            return 0.00
        return price

    try:
        with sqlite3.connect(db) as conn:

            df = pd.read_sql(
                f"SELECT * FROM '{table}' ORDER BY run_date_time DESC",
                con=conn,
            )

            df_concat = pd.concat([df_xls, df], ignore_index=True)
            df_concat.drop_duplicates(subset=["lot"], inplace=True)

            df_concat["item"] = df_concat["item"].astype(str)
            df_concat["lot"] = df_concat["lot"].astype(int)
            df_concat["run_date_time"] = df_concat["run_date_time"].astype(str).str[:10]
            df_concat["qty"] = df_concat["qty"].apply(lambda x: convert_float_to_int(x))

            df_concat["avg_contract_price"] = df_concat.apply(
                lambda x: get_price_from_map(x["item"], x["avg_contract_price"]),
                axis=1,
            )

            df_concat["cost"] = df_concat.apply(
                lambda x: get_cost_from_map(x["item"], x["cost"]), axis=1
            )

            df_concat["avg_contract_price"] = (
                df_concat["avg_contract_price"].astype(float).round(2)
            )
            df_concat["cost"] = df_concat["cost"].astype(float).round(2)

            df_concat["gross_profit"].fillna(0, inplace=True)
            df_concat["margin"].fillna(0, inplace=True)

            df_concat["gross_profit"] = df_concat.apply(
                lambda x: calculate_gross_profit(x["avg_contract_price"], x["cost"]),
                axis=1,
            )
            df_concat["margin"] = df_concat.apply(
                lambda x: calculate_gross_margin(x["avg_contract_price"], x["cost"]),
                axis=1,
            )

            df_concat["gross_profit"] = df_concat["gross_profit"].astype(float).round(2)
            df_concat["margin"] = df_concat["margin"].astype(float).round(2)

            df_concat["wc"] = df_concat.apply(
                lambda x: get_workcenter(x["wc"], x["item"]), axis=1
            )

            df_concat["order_value"] = df_concat.apply(
                lambda x: calculate_order_value(x["qty"], x["avg_contract_price"]),
                axis=1,
            )
            df_concat["order_value"] = df_concat["order_value"].astype(float).round(2)

            df_concat["order_cost"] = df_concat.apply(
                lambda x: calculate_order_cost(x["qty"], x["cost"]), axis=1
            )
            df_concat["order_cost"] = df_concat["order_cost"].astype(float).round(2)

            df_concat["order_profit"] = df_concat.apply(
                lambda x: calculate_order_profit(x["order_value"], x["order_cost"]),
                axis=1,
            )
            df_concat["order_profit"] = df_concat["order_profit"].astype(float).round(2)

            df_concat.to_sql(table, con=conn, if_exists="replace", index=False)

    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

    print("Updated schedule.db")

    return


def drop(table="Released Schedule"):
    with sqlite3.connect("schedule.db") as conn:
        conn.execute(f"DROP TABLE IF EXISTS '{table}'")


def get(limit: int = 1000):
    db = SCHEDULE_DB
    table = SCHEDULE_TABLE

    try:
        with sqlite3.connect(db) as conn:
            sql_query = f"SELECT * FROM '{table}' INNER JOIN 'parts' ON item=part ORDER BY run_date_time DESC LIMIT {limit}"
            if limit == -1:
                sql_query = f"SELECT * FROM '{table}' INNER JOIN 'parts' ON item=part ORDER BY run_date_time DESC"

            df = pd.read_sql(
                sql_query,
                con=conn,
            )
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

    return df[
        [
            "item",
            "lot",
            "run_date_time",
            "qty",
            "description",
        ]
    ]


def get_with_financials(limit: int = 1000):
    db = SCHEDULE_DB
    table = SCHEDULE_TABLE

    try:
        with sqlite3.connect(db) as conn:
            sql_query = f"SELECT * FROM '{table}' INNER JOIN 'parts' ON item=part ORDER BY run_date_time DESC LIMIT {limit}"
            if limit == -1:
                sql_query = f"SELECT * FROM '{table}' INNER JOIN 'parts' ON item=part ORDER BY run_date_time DESC"

            df = pd.read_sql(
                sql_query,
                con=conn,
            )

            df.fillna("", inplace=True)
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

    return df[
        [
            "item",
            "lot",
            "run_date_time",
            "qty",
            "description",
            "wc",
            "avg_contract_price",
            "cost",
            "gross_profit",
            "margin",
            "order_value",
            "order_cost",
            "order_profit",
        ]
    ]


if __name__ == "__main__":
    print("utility.py")
