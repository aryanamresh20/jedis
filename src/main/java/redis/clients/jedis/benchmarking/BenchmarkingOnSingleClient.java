package redis.clients.jedis.benchmarking;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BenchmarkingOnSingleClient {

    public static void main(String[] args) throws InterruptedException, IOException {

        InputStream inputStream = null;
        try {
            //config file for setting various properties
            if (args.length == 0) {
                String filePath = "default-benchmarking-config.properties";
                ClassLoader classLoader = BenchmarkingOnSingleClient.class.getClassLoader();
                inputStream = classLoader.getResourceAsStream(filePath);
            } else {
                inputStream = new FileInputStream(args[0]);
            }

            Properties props = new Properties();
            props.load(inputStream);

            //Assigning various properties to local parameters
            String hostName = props.getProperty("hostName");
            int portNumber = Integer.parseInt(props.getProperty("portNumber"));
            long numberOfKeys = Long.parseLong(props.getProperty("numberOfKeys"));
            long messageSize = Long.parseLong(props.getProperty("messageSize"));
            long expireAfterAccess = Long.parseLong(props.getProperty("expireAfterAccessMillis"));
            long expireAfterWrite = Long.parseLong(props.getProperty("expireAfterWriteMillis"));

            try {
                BenchmarkingUtil.populateKeys(hostName, portNumber, numberOfKeys, messageSize);
                BenchmarkingUtil.populateHashes(hostName, portNumber, numberOfKeys);

                //Comparing the time elapsed for reads where all reads comprise of cache misses
                ServerReadsGet serverReadsGet = new ServerReadsGet(hostName, portNumber, numberOfKeys,expireAfterAccess,expireAfterWrite);
                System.out.println("Single reads time Taken by normal jedis instance " + serverReadsGet.JedisTest());
                System.out.println("Single reads time Taken by CachedJedis instance " + serverReadsGet.CacheJedisTest());

                //Comparing the time elapsed for reads where most of the reads are from the cache
                CacheReadsGet cacheReadsGet = new CacheReadsGet(hostName, portNumber, numberOfKeys,expireAfterAccess,expireAfterWrite);
                System.out.println("Cache reads time Taken by normal jedis instance " + cacheReadsGet.JedisTest());
                System.out.println("Cache reads time Taken by CachedJedis instance " + cacheReadsGet.CacheJedisTest());

                //Comparing the time elapsed for mget reads with a mix of server and cache hits
                CacheReadsMget cacheReadsMget = new CacheReadsMget(hostName, portNumber, numberOfKeys,expireAfterAccess,expireAfterWrite);
                System.out.println("mget reads time taken by normal jedis instance " + cacheReadsMget.JedisTest());
                System.out.println("mget reads time taken by CachedJedis instance " + cacheReadsMget.CacheJedisTest());

                //Comparing the time elapsed for hget reads with a mix of server and cache hits
                CacheReadsHgetAll cacheReadsHgetAll = new CacheReadsHgetAll(hostName, portNumber, numberOfKeys,expireAfterAccess,expireAfterWrite);
                System.out.println("hget reads time taken by normal jedis instance " + cacheReadsHgetAll.JedisTest());
                System.out.println("hget reads time taken by CachedJedis instance " + cacheReadsHgetAll.CacheJedisTest());
            } finally {
                BenchmarkingUtil.cleanDatabase(hostName, portNumber);
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}
