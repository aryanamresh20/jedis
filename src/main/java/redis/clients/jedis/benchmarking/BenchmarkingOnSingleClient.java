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


                    //Comparing the time elapsed for hget reads with a mix of server and cache hits
                    BenchmarkHgetall benchmarkHgetall =
                            new BenchmarkHgetall(hostName, portNumber, numberOfKeys, expireAfterAccess, expireAfterWrite, warmCachePercentage);

                    jedisTime = benchmarkHgetall.getJedisRunningTime();
                    cacheJedisTime = benchmarkHgetall.getCachedJedisRunningTime(false);
                    cachedJedisWarmTime = benchmarkHgetall.getCachedJedisRunningTime(true);
                    list.set(3, list.get(3)+jedisTime);
                    list.set(4, list.get(4)+cacheJedisTime);
                    list.set(5, list.get(5)+cachedJedisWarmTime);

                    System.out.println("Cache hegtAll reads time Taken by normal jedis instance " +
                            jedisTime);
                    System.out.println("Cache hgetAll reads time Taken by CachedJedis instance " +
                            cacheJedisTime);
                    System.out.println("Cache hgetAll reads time Taken by CachedJedis instance " +
                            cachedJedisWarmTime);

                    //Comparing the time elapsed for mget reads with a mix of server and cache hits
                    BenchmarkMget benchmarkMget =
                            new BenchmarkMget(hostName, portNumber, numberOfKeys, expireAfterAccess, expireAfterWrite, warmCachePercentage);

                    jedisTime = benchmarkMget.getJedisRunningTime();
                    cacheJedisTime = benchmarkMget.getCachedJedisRunningTime(false);
                    cachedJedisWarmTime = benchmarkMget.getCachedJedisRunningTime(true);
                    list.set(6, list.get(6)+jedisTime);
                    list.set(7, list.get(7)+cacheJedisTime);
                    list.set(8, list.get(8)+cachedJedisWarmTime);

                    System.out.println("Cache mget reads time Taken by normal jedis instance " +
                            jedisTime);
                    System.out.println("Cache mget reads time Taken by CachedJedis instance " +
                            cacheJedisTime);
                    System.out.println("Cache mget reads time Taken by CachedJedis instance " +
                            cachedJedisWarmTime);
                    System.out.println("--------------------------------------------------------");
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
