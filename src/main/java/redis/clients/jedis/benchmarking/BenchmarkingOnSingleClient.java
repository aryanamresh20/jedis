package redis.clients.jedis.benchmarking;

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
            long warmCacheIterations = Long.parseLong(props.getProperty("warmCacheIterations"));

            try {
                BenchmarkingUtil.populateKeys(hostName, portNumber, numberOfKeys, messageSize);
                BenchmarkingUtil.populateHashes(hostName, portNumber, numberOfKeys, messageSize);

                //Comparing the time elapsed for reads where most of the reads are from the cache
                BenchmarkGet benchmarkGet =
                    new BenchmarkGet(hostName, portNumber, numberOfKeys, expireAfterAccess, expireAfterWrite, warmCacheIterations);
                System.out.println("Cache reads time Taken by normal jedis instance " +
                                   benchmarkGet.getJedisRunningTime());
                System.out.println("Cache reads time Taken by CachedJedis instance " +
                                   benchmarkGet.getCachedJedisRunningTime(false));
                System.out.println("Cache reads time Taken by CachedJedis instance " +
                                   benchmarkGet.getCachedJedisRunningTime(true));

                //Comparing the time elapsed for mget reads with a mix of server and cache hits
                BenchmarkMget benchmarkMget =
                    new BenchmarkMget(hostName, portNumber, numberOfKeys, expireAfterAccess, expireAfterWrite, warmCacheIterations);
                System.out.println("mget reads time taken by normal jedis instance " +
                                   benchmarkMget.getJedisRunningTime());
                System.out.println("mget reads time taken by CachedJedis instance " +
                                   benchmarkMget.getCachedJedisRunningTime(false));
                System.out.println("mget reads time taken by CachedJedis instance " +
                                   benchmarkMget.getCachedJedisRunningTime(true));

                //Comparing the time elapsed for hget reads with a mix of server and cache hits
                BenchmarkHgetall benchmarkHgetall =
                    new BenchmarkHgetall(hostName, portNumber, numberOfKeys, expireAfterAccess, expireAfterWrite, warmCacheIterations);
                System.out.println("hget reads time taken by normal jedis instance " +
                                   benchmarkHgetall.getJedisRunningTime());
                System.out.println("hget reads time taken by CachedJedis instance " +
                                   benchmarkHgetall.getCachedJedisRunningTime(false));
                System.out.println("hget reads time taken by CachedJedis instance " +
                                   benchmarkHgetall.getCachedJedisRunningTime(true));
            } finally {
                BenchmarkingUtil.cleanDatabase(hostName, portNumber);
            }
        } catch (Exception ex) {
            System.out.println("FAILED !!!!" + ex);
        }
    }
}
