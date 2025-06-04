# DeleteOldMarketPricesFunction Usage

## What It Does
Deletes MarketPrices entries older than a specified timestamp.

## Safety
- Only deletes data older than 2 years
- Processes data in batches to avoid memory issues
- Logs all operations

## How to Use

### 1. Deploy the JAR
```
gfsh> deploy --jar=/path/to/custom-functions.jar
```

### 2. Execute the Function
Basic usage (default 10,000 batch size):
```
gfsh> execute function --id=DeleteOldMarketPricesFunction --region=/MarketPrices --arguments=TIMESTAMP
```

With custom batch size:
```
gfsh> execute function --id=DeleteOldMarketPricesFunction --region=/MarketPrices --arguments=TIMESTAMP,BATCH_SIZE
```

## Examples

Delete entries older than January 1, 2020:
```
gfsh> execute function --id=DeleteOldMarketPricesFunction --region=/MarketPrices --arguments=1577836800000
```

Delete with 50,000 batch size for faster processing:
```
gfsh> execute function --id=DeleteOldMarketPricesFunction --region=/MarketPrices --arguments=1577836800000,50000
```

## Getting Timestamps

Current time minus 2 years in milliseconds:
- Today (June 3, 2025): 1748893011803
- 2 years ago: 1685821011803

Convert dates at: https://www.epochconverter.com/

## Monitoring

Check remaining entries:
```
gfsh> query --query="SELECT COUNT(*) FROM /MarketPrices"
```

View logs on each server for detailed progress.