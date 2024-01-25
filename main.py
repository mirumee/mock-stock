import asyncio
import math
from contextlib import asynccontextmanager
from random import randint
from typing import Annotated

import aiosqlite
import httpx
from faker import Faker
from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import StreamingResponse, Response
import structlog

fake = Faker()

log = structlog.get_logger()


async def send_requests(
    webhook_url,
    stocks_to_update: list,
    concurrency: int = 1,
    sleep: float = 0,
    duplications_number=0,
):
    async with httpx.AsyncClient() as client:
        grouped_by_concurrency = [
            stocks_to_update[i : i + concurrency]
            for i in range(0, len(stocks_to_update), concurrency)
        ]
        for i in range(duplications_number + 1):
            for group in grouped_by_concurrency:
                r = []
                for s in group:
                    data = {
                        "id": s[3],
                        "sku": s[0],
                        "value": s[1],
                        "modified_since": s[2].isoformat(),
                    }
                    r.append(client.post(webhook_url, json=data))
                await asyncio.gather(*r)
                await asyncio.sleep(sleep)


async def generate_random_data(db, amount, offset=1000):
    ii = math.ceil(amount / offset)
    left = amount % offset
    for i in range(ii):
        data = []
        if i + 1 == ii:
            _offset = left
        else:
            _offset = offset
        for j in range(_offset):
            date = fake.date_time_this_month()
            sku = fake.ean(length=13)
            data.append((sku, randint(0, 200), date))
            yield (sku, randint(0, 200), date)
        await db.executemany("""INSERT INTO Stock VALUES (null, ?, ?, ?)""", data)
        data = []
    await db.commit()


async def change_stock_randomly(db, amount):
    data = []
    async with app.db.execute(
        "SELECT id, sku FROM Stock ORDER BY RANDOM() LIMIT ?;", (amount,)
    ) as cur:
        async for stock_id, sku in cur:
            date = fake.date_time_between(start_date="-1m", end_date="now")
            data.append((sku, randint(0, 200), date, stock_id))

    await db.executemany(
        "UPDATE Stock SET sku=?, value=?, modified_since=? WHERE id=?", data
    )
    await db.commit()
    return data


@asynccontextmanager
async def db_lifespan(app: FastAPI):
    app.db = await aiosqlite.connect("stock.db")
    yield
    await app.db.close()


app = FastAPI(lifespan=db_lifespan)
app.db: aiosqlite.Connection = None


@app.get("/")
async def read_root():
    """
    Endpoint returns CSV formatted list of all stocks.

    Useful for whole stock synchronization.
    """

    async def stream_data():
        yield "id,sku,value,modified_since\n"
        async with app.db.execute(
            "SELECT id, sku, value, modified_since FROM Stock ORDER BY modified_since"
        ) as cur:
            async for row in cur:
                yield ",".join((str(r) for r in row)) + "\n"

    return StreamingResponse(stream_data(), media_type="text/csv")


@app.post("/trigger/")
async def stock_trigger(
    number_to_change: Annotated[
        int, Form(description="Number of random stock for update")
    ],
    background_tasks: BackgroundTasks,
    webhook_url: Annotated[
        str,
        Form(description="If presented changes will be send as a separate request."),
    ] = None,
    concurrency: Annotated[
        int, Form(description="How many requests should be send at once. Default 1.")
    ] = 1,
    sleep: Annotated[
        float,
        Form(description="How long wait before next batch of requests. Default 0."),
    ] = 0,
    duplicate: Annotated[
        int, Form(description="Duplicate stocks update requests. Default 0.")
    ] = 0,
):
    """
    Endpoint returns randomly changed stock based on post params.
    If the webhook_url body param presented also sends changes to the provided URL.
    """
    stocks = await change_stock_randomly(app.db, number_to_change)

    if webhook_url:
        background_tasks.add_task(
            send_requests, webhook_url, stocks, concurrency, sleep, duplicate
        )

    def stream_data():
        yield "id,sku,value,modified_since\n"
        for s in stocks:
            s = (str(s[3]), s[0], str(s[1]), s[2].isoformat())
            yield ",".join(s) + "\n"

    return StreamingResponse(stream_data(), media_type="text/csv")


@app.post("/initialize-stock/")
async def initialize_stock(
    amount: Annotated[int, Form(description="How may records you want to generate.")]
):
    """
    Endpoint generates stocks.

    By sending the amount value you can control how may records you want to generate.
    """
    await app.db.execute("DELETE FROM Stock")
    await app.db.execute(
        """
        CREATE TABLE IF NOT EXISTS Stock(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT,
                value INT,
                modified_since DATETIME)
        """
    )
    await app.db.execute("UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME='Stock'")
    await app.db.commit()

    async def stream_data():
        yield "id,sku,value,modified_since\n"
        _id = 1
        async for s in generate_random_data(app.db, amount):
            s = (str(_id), s[0], str(s[1]), s[2].isoformat())
            _id += 1
            yield ",".join(s) + "\n"

    return StreamingResponse(stream_data(), media_type="text/csv")


@app.post("/receiver/")
async def receiver(request: Request, status_code: int = 200):
    """
    Endpoint for testing webhooks.

    By setting status_code get param you can control request status.
    Good option for checking how integration works with different statuses.
    """
    content = await request.body()
    log.info("Receiver request", body=content)
    return Response(content=content, status_code=status_code)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
