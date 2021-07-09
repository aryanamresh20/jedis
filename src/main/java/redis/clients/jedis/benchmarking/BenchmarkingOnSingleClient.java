package redis.clients.jedis.benchmarking;



import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class BenchmarkingOnSingleClient {

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

        //Comparing the time elapsed for reads where all reads comprise of cache misses
        ServerReadsGet serverReadsGet = new ServerReadsGet(hostName,portNumber,numberOfKeys);
        System.out.println("Single reads time Taken by normal jedis instance "+ serverReadsGet.JedisTest());
        System.out.println("Single reads time Taken by CachedJedis instance "+ serverReadsGet.CacheJedisTest());

        //Comparing the time elapsed for reads where most of the reads are from the cache
        CacheReadsGet cacheReadsGet = new CacheReadsGet(hostName,portNumber,numberOfKeys);
        System.out.println("Cache reads time Taken by normal jedis instance "+ cacheReadsGet.JedisTest());
        System.out.println("Cache reads time Taken by CachedJedis instance "+ cacheReadsGet.CacheJedisTest());

        //Comparing the time elapsed for mget reads with a mix of server and cache hits
        CacheReadsMget cacheReadsMget = new CacheReadsMget(hostName,portNumber,numberOfKeys);
        System.out.println("mget reads time taken by normal jedis instance "+ cacheReadsMget.JedisTest());
        System.out.println("mget reads time taken by CachedJedis instance "+ cacheReadsMget.CacheJedisTest());

        //Comparing the time elapsed for hget reads with a mix of server and cache hits
        CacheReadsHgetAll cacheReadsHgetAll = new CacheReadsHgetAll(hostName,portNumber,numberOfKeys);
        System.out.println("hget reads time taken by normal jedis instance "+ cacheReadsHgetAll.JedisTest());
        System.out.println("hget reads time taken by CachedJedis instance "+ cacheReadsHgetAll.CacheJedisTest());
    }
}
