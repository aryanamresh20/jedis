package redis.clients.jedis.benchmarking;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class BenchmarkingMain {

    public static void main(String args[]) throws FileNotFoundException,InterruptedException,IOException {
        //config file for setting various properties
        String filePath = "/Users/aryanamresh/Documents/jedis/out/artifacts/jedis_jar/config.properties";
        Properties props = new Properties();
        FileInputStream ip = new FileInputStream(filePath);
        props.load(ip);

        //Assigning various properties to local parameters
        String hostName = props.getProperty("hostName");
        int portNumber = Integer.parseInt(props.getProperty("portNumber"));
        int numberOfKeys = Integer.parseInt(props.getProperty("numberOfKeys"));
        int numberOfClients = Integer.parseInt(props.getProperty("numberOfClients"));
        int readPercentage = Integer.parseInt(props.getProperty("readPercentage"));
        int writePercentage = Integer.parseInt(props.getProperty("writePercentage"));
        int numberOfOperations = Integer.parseInt(props.getProperty("numberOfOperations"));
        int meanOperationTime = Integer.parseInt(props.getProperty("meanOperationTime"));
        double sigmaOperationTime = Double.parseDouble(props.getProperty("sigmaOperationTime"));
        boolean flag = true;

        //Comparing the time elapsed for reads where all reads comprise of cache misses
        SingleReads singleReads = new SingleReads(hostName,portNumber,numberOfKeys);
        System.out.println("Single reads time Taken by normal jedis instance "+singleReads.JedisTest());
        System.out.println("Single reads time Taken by CacheJedis instance "+singleReads.CacheJedisTest());

        //Comparing the time elapsed for reads where most of the reads are from the cache
        CacheReads cacheReads = new CacheReads(hostName,portNumber,numberOfKeys);
        System.out.println("Cache reads time Taken by normal jedis instance "+cacheReads.JedisTest());
        System.out.println("Cache reads time Taken by CacheJedis instance "+cacheReads.CacheJedisTest());

        //Comparing the time elapsed for mget reads with a mix of server and cache hits
        ReadsMget readsMget = new ReadsMget(hostName,portNumber,numberOfKeys);
        System.out.println("mget reads time taken by normal jedis instance "+readsMget.JedisTest());
        System.out.println("mget reads time taken by normal CacheJedis instance "+readsMget.CacheJedisTest());

        //Evaluating various parameters on multi CacheJedis clients Stale values , Cache Misses e.t.c on various parameters
        CountStaleValues countStaleValues = new CountStaleValues(hostName,portNumber,numberOfClients,numberOfKeys,readPercentage,writePercentage,numberOfOperations,meanOperationTime,sigmaOperationTime);
        Thread thread = Thread.currentThread();
        //Waiting for all other threads to close
        while(flag) {
            thread.sleep(100);
            if(thread.activeCount()==2){
                flag=false;
            }
        }

        System.out.println("Stale values "+countStaleValues.getStaleCount());
        System.out.println("Cache Hits "+countStaleValues.getCacheHit());
        System.out.println("Cache Misses "+countStaleValues.getCacheMiss());

    }
}
