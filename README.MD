# DeleteOldMarketPricesFunction Usage

## What It Does
Deletes MarketPrices region entries using the priceTimstamp field that are older than a specified timestamp.

## Safety
- Only deletes data older than 2 years
- Processes data in batches to avoid memory issues
- Logs all operations onto the cache server logs

## Input
- (Required) You must provide a timestamp as an argument in the form of a unix timestamp that includes milliseconds. Example: 1748895985671
- (Optional) Batch Size. Defaults to deleting 10000 at a time, but can be increased or decreased as needed.

## Compile and Package

### Prequisites
- Appropriate JDK installed to compile and package file
- GemFire installed and $GEMFIRE_HOME set on path.

### Compile and Package
Run the compile-custom-functions.sh script to compile DeleteOldMarketPricesFunction.java and build custom-functions.jar. These files will be in the custom-functions/target directory if successful. 

```
./compile-custom-functions.sh
```

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