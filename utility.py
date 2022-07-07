import msoffcrypto, sqlite3, os
import pandas as pd
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

print(SCHEDULE_DB, SCHEDULE_XLS, SCHEDULE_TABLE, ENCRYPTION_PASSWORD)


def convert_float_to_int(nbr: str) -> int:
    try:
        return int(nbr)
    except ValueError:
        nbr = nbr.replace(",", "").split(".")[0]
        return int(nbr)


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
        ["item", "lot", "run_date_time", "qty"]
    ].copy()

    df = df.loc[~df.lot.str.contains(r"[A-Z]")].copy()
    df.item = df.item.astype(str)
    df.lot = df.lot.astype(int)
    df.run_date_time = df.run_date_time.astype(str)
    df.qty = df.qty.apply(lambda x: convert_float_to_int(x))

    return df


def update(df_xls: pd.DataFrame):
    db = SCHEDULE_DB
    table = SCHEDULE_TABLE

    try:
        with sqlite3.connect(db) as conn:

            df = pd.read_sql(
                f"SELECT * FROM '{table}' ORDER BY run_date_time DESC",
                con=conn,
            )

            df_concat = pd.concat([df, df_xls], ignore_index=True)
            df_concat.drop_duplicates(subset=["lot"], inplace=True)

            df_concat.item = df_concat.item.astype(str)
            df_concat.lot = df_concat.lot.astype(int)
            df_concat.run_date_time = df_concat.run_date_time.astype(str).str[:10]
            df_concat.qty = df_concat.qty.apply(lambda x: convert_float_to_int(x))

            df_concat.to_sql(table, con=conn, if_exists="replace", index=False)

    except sqlite3.Error as e:
        print(e)


def drop(table="Released Schedule"):
    with sqlite3.connect("schedule.db") as conn:
        conn.execute(f"DROP TABLE IF EXISTS '{table}'")


def get(limit: int = 1000):
    db = SCHEDULE_DB
    table = SCHEDULE_TABLE

    try:
        with sqlite3.connect(db) as conn:
            sql_query = (
                f"SELECT * FROM '{table}' ORDER BY run_date_time DESC LIMIT {limit}"
            )
            if limit == -1:
                sql_query = f"SELECT * FROM '{table}' ORDER BY run_date_time DESC"

            df = pd.read_sql(
                sql_query,
                con=conn,
            )

    except sqlite3.Error as e:
        print(e)

    return df


if __name__ == "__main__":
    print(os.getcwd())
