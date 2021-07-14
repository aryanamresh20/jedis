package redis.clients.jedis.benchmarking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class BenchmarkingOnSingleClient {

    public static void main(String[] args) {
        try {
            Properties props = BenchmarkingUtil.loadConfigFile(args);

            //Assigning various properties to local parameters
            String hostName = props.getProperty("hostName");
            int portNumber = Integer.parseInt(props.getProperty("portNumber"));
            long numberOfKeys = Long.parseLong(props.getProperty("numberOfKeys"));
            long messageSize = Long.parseLong(props.getProperty("messageSize"));
            long expireAfterAccess = Long.parseLong(props.getProperty("expireAfterAccessMillis"));
            long expireAfterWrite = Long.parseLong(props.getProperty("expireAfterWriteMillis"));
            long warmCachePercentage = Long.parseLong(props.getProperty("warmCachePercentage"));

            try {
                List<Double> list = new ArrayList<>(Collections.nCopies(9, 0.0));
                for(int i =0 ; i<3 ; i++) {
                    long jedisTime , cacheJedisTime , cachedJedisWarmTime;
                    BenchmarkingUtil.populateKeys(hostName, portNumber, numberOfKeys, messageSize);
                    BenchmarkingUtil.populateHashes(hostName, portNumber, numberOfKeys, messageSize);
                    //Comparing the time elapsed for reads where most of the reads are from the cache
                    BenchmarkGet benchmarkGet =
                            new BenchmarkGet(hostName, portNumber, numberOfKeys, expireAfterAccess, expireAfterWrite, warmCachePercentage);
                    jedisTime = benchmarkGet.getJedisRunningTime();
                    cacheJedisTime = benchmarkGet.getCachedJedisRunningTime(false);
                    cachedJedisWarmTime = benchmarkGet.getCachedJedisRunningTime(true);
                    list.set(0, list.get(0)+jedisTime);
                    list.set(1, list.get(1)+cacheJedisTime);
                    list.set(2, list.get(2)+cachedJedisWarmTime);
                    System.out.println("Cache get reads time Taken by normal jedis instance " +
                           jedisTime);
                    System.out.println("Cache get reads time Taken by CachedJedis instance " +
                            cacheJedisTime);
                    System.out.println("Cache get reads time Taken by CachedJedis instance " +
                            cachedJedisWarmTime);

                }
                System.out.println("Average Times");
                for(int i =0 ;i < 9 ; i++){
                    System.out.println(list.get(i)/3.0);
                }
            } finally {
                BenchmarkingUtil.cleanDatabase(hostName, portNumber);
            }
        } catch (Exception ex) {
            System.out.println("FAILED !!!!" + ex);
        }
    }
}
