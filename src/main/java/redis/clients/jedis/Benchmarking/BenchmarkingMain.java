package redis.clients.jedis.Benchmarking;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class BenchmarkingMain {

    public static void main(String args[]) throws FileNotFoundException {
        String filePath = "src/main/java/redis/clients/jedis/Benchmarking/config.properties";
        Properties props = new Properties();
        FileInputStream ip = new FileInputStream(filePath);
        try {
            props.load(ip);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String hostName = props.getProperty("hostName");
        int portNumber = Integer.parseInt(props.getProperty("portNumber"));
        int numberOfKeys = Integer.parseInt(props.getProperty("numberOfKeys"));
        int numberOfClients = Integer.parseInt(props.getProperty("numberOfClients"));
        int readPercentage = Integer.parseInt(props.getProperty("readPercentage"));
        int writePercentage = Integer.parseInt(props.getProperty("writePercentage"));
        int numberOfOperations = Integer.parseInt(props.getProperty("numberOfOperations"));

        //Comparing the Reads where all reads comprise of cache misses
        SingleReads singleReads = new SingleReads(hostName,portNumber,numberOfKeys);
        System.out.println("Single reads time Taken by normal jedis instance "+singleReads.JedisTest());
        System.out.println("Single reads time Taken by CacheJedis instance "+singleReads.CacheJedisTest());

        //Comparing the reads where most of the reads are from the cache
        CacheReads cacheReads = new CacheReads(hostName,portNumber,numberOfKeys);
        System.out.println("Cache reads time Taken by normal jedis instance "+cacheReads.JedisTest());
        System.out.println("Cache reads time Taken by CacheJedis instance "+cacheReads.CacheJedisTest());

        CountStaleValues countStaleValues = new CountStaleValues(hostName,portNumber,numberOfClients,numberOfKeys,readPercentage,writePercentage,numberOfOperations);
        Thread thread = Thread.currentThread();
        try{
            thread.sleep(400);
        }
        catch(InterruptedException e) {
        }{

        }
        System.out.println("Stale values "+countStaleValues.getStaleCount());
        System.out.println("Cache Hits "+countStaleValues.getCacheHit());
        System.out.println("Cache Misses "+countStaleValues.getCacheMiss());

    }
}
