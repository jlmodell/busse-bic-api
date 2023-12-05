from datetime import datetime
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from utility import unencrypt_excel, update, get, get_with_financials
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def update_lots() -> None:
    df = unencrypt_excel()

    update(df)

    now = datetime.now()

    with open(os.path.join(os.getcwd(), "logs", "updates.log"), "a") as f:
        f.write("Updated lots at {:%Y-%m-%d %H:%M:%S}\n".format(now))

    print("Updated lots at {:%Y-%m-%d %H:%M:%S}\n".format(now))

    return


@app.on_event("startup")
async def hooks():
    await update_lots()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        func=update_lots,
        trigger="interval",
        seconds=60 * 60,
    )
    scheduler.start()


@app.get("/")
def read_root():
    return {"appname": "bic-api-refresher", "version": "0.1.0"}


@app.get("/api/bic/refresh")
async def refresher():
    try:
        df_xls = unencrypt_excel()
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "function": "unencrypt_excel"},
        )

    try:
        update(df_xls)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "function": "update"},
        )

    return JSONResponse(content={"status": "success"})


@app.get("/api/bic/data")
async def decrypt_and_return_dict(limit: int = 1000):
    df = get(limit).to_dict(orient="records")

    return JSONResponse(content=df)


@app.get("/api/schedule/data")
async def decrypt_and_return_dict(limit: int = 1000):
    df = get_with_financials(limit).to_dict(orient="records")

    return JSONResponse(content=df)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=True)
