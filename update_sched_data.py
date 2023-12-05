from db import contracts, costs, sched_data
import pandas as pd

def update_sd2():
    file = "c:\\temp\\sd2.tsv"

    df = pd.read_csv(file, sep="\t", dtype=str, header=None)
    df = df[[0,1,2]]
    df.columns = ["part", "description", "wc"]

    df.dropna(subset=["description"], inplace=True)
    df.fillna("", inplace=True)

    sd = {x["part"]:x for x in list(sched_data.find({}, {"_id": 0}))}

    for i, row in df.iterrows():
        print(f"{i} of {len(df)}")
        
        if row["part"] in sd:            
            sched_data.update_one({"part": row["part"]}, {"$set": {"wc": row["wc"]}})
        else:
            sched_data.insert_one({
                "part": row["part"],
                "description": row["description"],
                "wc": row["wc"],
            })

    return df

if __name__ == "__main__":
    df = update_sd2()