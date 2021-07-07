package redis.clients.jedis.benchmarking;



import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class BenchmarkingMain {

    public static void main(String args[]) throws InterruptedException,IOException {
        //config file for setting various properties
        String filePath = "/Users/aryanamresh/Documents/jedis/out/artifacts/jedis_jar/config.properties";
        Properties props = new Properties();
        FileInputStream ip = new FileInputStream(filePath);
        props.load(ip);

        //Assigning various properties to local parameters
        String hostName = props.getProperty("hostName");
        int portNumber = Integer.parseInt(props.getProperty("portNumber"));
        long numberOfKeys = Long.parseLong(props.getProperty("numberOfKeys"));
        long numberOfClients = Long.parseLong(props.getProperty("numberOfClients"));
        long readPercentage = Long.parseLong(props.getProperty("readPercentage"));
        long writePercentage = Long.parseLong(props.getProperty("writePercentage"));
        long numberOfOperations = Long.parseLong(props.getProperty("numberOfOperations"));
        long meanOperationTime = Long.parseLong(props.getProperty("meanOperationTime"));
        long expireAfterAccess = Long.parseLong(props.getProperty("expireAfterAccessMillis"));
        long expireAfterWrite = Long.parseLong(props.getProperty("expireAfterWriteMillis"));
        long initialCachePopulate = Long.parseLong(props.getProperty("initialCachePopulationIters"));
        long messageSize = Long.parseLong(props.getProperty("messageSize"));
        long readFromGroup = Long.parseLong(props.getProperty("readFromGroup"));
        double sigmaOperationTime = Double.parseDouble(props.getProperty("sigmaOperationTime"));

        //Comparing the time elapsed for reads where all reads comprise of cache misses
        SingleReads singleReads = new SingleReads(hostName,portNumber,numberOfKeys);
        System.out.println("Single reads time Taken by normal jedis instance "+singleReads.JedisTest());
        System.out.println("Single reads time Taken by CachedJedis instance "+singleReads.CacheJedisTest());

        //Comparing the time elapsed for reads where most of the reads are from the cache
        CacheReads cacheReads = new CacheReads(hostName,portNumber,numberOfKeys);
        System.out.println("Cache reads time Taken by normal jedis instance "+cacheReads.JedisTest());
        System.out.println("Cache reads time Taken by CachedJedis instance "+cacheReads.CacheJedisTest());

        //Comparing the time elapsed for mget reads with a mix of server and cache hits
        ReadsMget readsMget = new ReadsMget(hostName,portNumber,numberOfKeys);
        System.out.println("mget reads time taken by normal jedis instance "+readsMget.JedisTest());
        System.out.println("mget reads time taken by CachedJedis instance "+readsMget.CacheJedisTest());

        //Comparing the time elapsed for hget reads with a mix of server and cache hits
        ReadsHgetAll readsHgetAll = new ReadsHgetAll(hostName,portNumber,numberOfKeys);
        System.out.println("hget reads time taken by normal jedis instance "+ readsHgetAll.JedisTest());
        System.out.println("hget reads time taken by CachedJedis instance "+ readsHgetAll.CacheJedisTest());

        //Evaluating various parameters on multi CachedJedis clients Stale values , Cache Misses e.t.c on various parameters
        CachedJedisLatencies cachedJedisLatencies = new CachedJedisLatencies(hostName,portNumber,numberOfClients,numberOfKeys,
                                                readPercentage,writePercentage,numberOfOperations,meanOperationTime,
                                                sigmaOperationTime,expireAfterAccess,expireAfterWrite,messageSize,initialCachePopulate,
                                                readFromGroup);

        System.out.println("Stale values "+ cachedJedisLatencies.getStaleCount());
        System.out.println("Cache Hits "+ cachedJedisLatencies.getCacheHit());
        System.out.println("Cache Misses "+ cachedJedisLatencies.getCacheMiss());
        cachedJedisLatencies.getLatencies();

    }
}
