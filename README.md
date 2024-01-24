# CRM stock mocker

MockStock Generator is a versatile and dynamic stock update mocking service designed specifically for testing AWS infrastructure. This service provides a unique solution for developers and testers who require a realistic simulation of stock market data. It enables the creation of stock datasets of any size, complete with the capability to introduce random fluctuations in stock values. Additionally, the service integrates seamless webhook notifications for real-time data updates, ensuring a comprehensive testing environment for AWS applications.

All endpoints documentation is available in the http://localhost:8000/docs

## How to run

Install necessary packages:
```
pip install -r requirements.txt
```

Run script by:
```
python main.py
```

## Usecases

Get all stocks in CSV format:
```
GET http://localhost:8000/
```

Generate 100 000k stock mock:
```
POST http://localhost:8000/initialize-stock/
amount=100000
```

Change 100 stocks randomly, and return changed in CSV:
```
POST http://localhost:8000/trigger/
number_to_change=100
```

Change 1000 stocks randomly, and trigger 10 webhooks at the time and wait 1s for the next batch:
```
POST http://localhost:8000/trigger/
number_to_change=1000&
webhook_url=http://localhost:8000/test/&
concurrency=10&
sleep=1
```

Change 1 stocks randomly, and trigger 2 same webhooks:
```
POST http://localhost:8000/trigger/
number_to_change=1000&
webhook_url=http://localhost:8000/test/&
duplicate=1
```
