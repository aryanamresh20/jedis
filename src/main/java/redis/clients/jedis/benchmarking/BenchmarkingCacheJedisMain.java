package redis.clients.jedis.benchmarking;

import java.util.Properties;

public class BenchmarkingCacheJedisMain {

    public static void main(String[] args) {
        try {
            Properties props = BenchmarkingUtil.loadConfigFile(args);

            //Assigning various properties to local parameters
            String hostName = props.getProperty("hostName");
            int portNumber = Integer.parseInt(props.getProperty("portNumber"));
            long numberOfKeys = Long.parseLong(props.getProperty("numberOfKeys"));
            long numberOfClients = Long.parseLong(props.getProperty("numberOfClients"));
            long writePercentage = Long.parseLong(props.getProperty("writePercentage"));
            long numberOfOperations = Long.parseLong(props.getProperty("numberOfOperations"));
            long meanOperationTime = Long.parseLong(props.getProperty("meanOperationTime"));
            long expireAfterAccess = Long.parseLong(props.getProperty("expireAfterAccessMillis"));
            long expireAfterWrite = Long.parseLong(props.getProperty("expireAfterWriteMillis"));
            long warmCacheIterations = Long.parseLong(props.getProperty("warmCacheIterations"));
            long messageSize = Long.parseLong(props.getProperty("messageSize"));
            long readFromGroup = Long.parseLong(props.getProperty("readFromGroup"));
            long sigmaOperationTime = Long.parseLong(props.getProperty("sigmaOperationTime"));

            BenchmarkingUtil.populateKeys(hostName, portNumber, numberOfKeys, messageSize);


            CachedJedisLatencies cachedJedisLatencies =
                new CachedJedisLatencies(hostName, portNumber, writePercentage, numberOfClients,
                                         numberOfKeys, numberOfOperations, sigmaOperationTime, meanOperationTime,
                                         expireAfterAccess, expireAfterWrite, messageSize, warmCacheIterations,
                                         readFromGroup, true);
            cachedJedisLatencies.beginBenchmark();

            System.out.println("Stale values " + cachedJedisLatencies.getStaleCount());
            System.out.println("Cache Hits " + cachedJedisLatencies.getCacheHit());
            System.out.println("Cache Misses " + cachedJedisLatencies.getCacheMiss());
            System.out.println("--------------------------------------------------------------");
            //To get overall latencies
            cachedJedisLatencies.getOverallLatencies();
            //To get latencies of the server hits
            cachedJedisLatencies.getServerLatencies();
            //To get latencies of the cache hits
            cachedJedisLatencies.getCacheLatencies();
        } catch (Exception ex) {
            System.out.println("FAILED !!!!" + ex);
        }
    }
}
