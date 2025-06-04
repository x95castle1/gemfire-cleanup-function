import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.pdx.PdxInstance;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DeleteOldMarketPricesFunction implements Function {

    private static final Logger logger = LogManager.getLogger(DeleteOldMarketPricesFunction.class);
    private static final long serialVersionUID = 1L;
    private static final int BATCH_SIZE = 10000;

    @Override
    public void execute(FunctionContext context) {
        String memberName = context.getMemberName();
        logger.info("{}: Executing {}", memberName, getId());

        RegionFunctionContext rfc = (RegionFunctionContext) context;
        Region<Object, Object> localRegion = PartitionRegionHelper.getLocalDataForContext(rfc);

        // Get the cutoff timestamp from function arguments
        Object[] args = (Object[]) rfc.getArguments();
        if (args == null || args.length == 0) {
            context.getResultSender().lastResult("ERROR: No timestamp argument provided");
            return;
        }

        // Handle both String and Long arguments
        long cutoffTimestamp;
        if (args[0] instanceof String) {
            cutoffTimestamp = Long.parseLong((String) args[0]);
        } else if (args[0] instanceof Long) {
            cutoffTimestamp = (Long) args[0];
        } else {
            context.getResultSender().lastResult("ERROR: Invalid timestamp argument type: " + args[0].getClass().getName());
            return;
        }

        // Optional batch size parameter (default: 10000)
        int batchSize = BATCH_SIZE;
        if (args.length > 1) {
            if (args[1] instanceof String) {
                batchSize = Integer.parseInt((String) args[1]);
            } else if (args[1] instanceof Number) {
                batchSize = ((Number) args[1]).intValue();
            }
            logger.info("{}: Using custom batch size: {}", memberName, batchSize);
        }

        // Safety check: Ensure cutoff is at least 2 years in the past
        long now = System.currentTimeMillis();
        long twoYearsAgo = now - (2 * 365L * 24 * 60 * 60 * 1000);

        if (cutoffTimestamp > twoYearsAgo) {
            long daysUntilAllowed = (cutoffTimestamp - twoYearsAgo) / (24 * 60 * 60 * 1000);
            String errorMsg = String.format(
                    "ERROR: Safety check failed! Cutoff timestamp %d is too recent (%d days before the 2-year safety limit). " +
                            "This function only deletes data older than 2 years. Current time: %d, Two years ago: %d",
                    cutoffTimestamp, daysUntilAllowed, now, twoYearsAgo
            );
            logger.error(errorMsg);
            context.getResultSender().lastResult(errorMsg);
            return;
        }

        logger.info("{}: Safety check passed. Cutoff {} is {} days in the past",
                memberName, cutoffTimestamp, (now - cutoffTimestamp) / (24 * 60 * 60 * 1000));

        int totalEntries = localRegion.size();
        AtomicLong deletedCount = new AtomicLong(0);
        AtomicLong scannedCount = new AtomicLong(0);

        logger.info("{}: Starting deletion for entries older than {} in region with {} entries",
                memberName, cutoffTimestamp, totalEntries);

        long startTime = System.currentTimeMillis();

        // Collect keys to delete in batches
        List<Object> keysToDelete = new ArrayList<>(batchSize);

        try {
            // Iterate through all entries in the local data
            for (Object entryObj : localRegion.entrySet()) {
                Region.Entry<Object, Object> entry = (Region.Entry<Object, Object>) entryObj;
                scannedCount.incrementAndGet();

                Object value = entry.getValue();
                // Assuming the value object has a getPriceTimestamp() method
                // Adjust this based on your actual value object structure
                if (value != null) {
                    long priceTimestamp = extractPriceTimestamp(value);

                    if (priceTimestamp < cutoffTimestamp) {
                        keysToDelete.add(entry.getKey());

                        // Delete in batches
                        if (keysToDelete.size() >= batchSize) {
                            deleteBatch(localRegion, keysToDelete, deletedCount);
                        }
                    }
                }

                // Progress reporting based on batch size
                // Report every 10 batches or 100k entries, whichever is smaller
                int progressInterval = Math.min(batchSize * 10, 100000);
                if (scannedCount.get() % progressInterval == 0) {
                    logger.info("{}: Progress - Scanned: {}, Deleted: {} (batch size: {})",
                            memberName, scannedCount.get(), deletedCount.get(), batchSize);
                }
            }

            // Delete any remaining keys
            if (!keysToDelete.isEmpty()) {
                deleteBatch(localRegion, keysToDelete, deletedCount);
            }

        } catch (Exception e) {
            logger.error("Error during deletion", e);
            context.getResultSender().lastResult("ERROR: " + e.getMessage());
            return;
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        String result = String.format(
                "%s: Completed - Scanned %d entries, Deleted %d entries in %d ms (%.2f entries/sec)",
                memberName,
                scannedCount.get(),
                deletedCount.get(),
                duration,
                (deletedCount.get() * 1000.0) / duration
        );

        logger.info(result);

        List<String> resultList = new ArrayList<>();
        resultList.add(result);
        context.getResultSender().lastResult(resultList);
    }

    private void deleteBatch(Region<Object, Object> region, List<Object> keysToDelete, AtomicLong deletedCount) {
        try {
            // Use removeAll for batch deletion - much faster than individual removes
            region.removeAll(keysToDelete);
            deletedCount.addAndGet(keysToDelete.size());
            keysToDelete.clear();
        } catch (Exception e) {
            // Fallback to individual deletes if batch fails
            logger.warn("Batch delete failed, falling back to individual deletes", e);
            for (Object key : keysToDelete) {
                try {
                    region.remove(key);
                    deletedCount.incrementAndGet();
                } catch (Exception ex) {
                    logger.error("Failed to delete key {}", key, ex);
                }
            }
            keysToDelete.clear();
        }
    }

    /**
     * Extract priceTimestamp from the value object.
     * Adjust this method based on your actual value object structure.
     */
    private long extractPriceTimestamp(Object value) {
        // Option 1: If value has a getter method
        // return ((MarketPrice) value).getPriceTimestamp();

        // Option 2: If using reflection (slower but more flexible)
        try {
            java.lang.reflect.Method method = value.getClass().getMethod("getPriceTimestamp");
            return (Long) method.invoke(value);
        } catch (Exception e) {
            // Option 3: If value is a PDX instance
            if (value instanceof PdxInstance) {
                PdxInstance pdx = (PdxInstance) value;
                return ((Number) pdx.getField("priceTimestamp")).longValue();
            }

            logger.warn("Could not extract priceTimestamp from value", e);
            return Long.MAX_VALUE; // Don't delete if we can't read timestamp
        }
    }

    @Override
    public String getId() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean optimizeForWrite() {
        return true;
    }

    @Override
    public boolean hasResult() {
        return true;
    }

    @Override
    public boolean isHA() {
        return true;
    }
}