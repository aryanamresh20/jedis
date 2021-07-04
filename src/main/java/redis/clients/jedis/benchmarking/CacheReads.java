package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CacheConfig;
import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;

import java.util.Calendar;

public class CacheReads {

    private String hostName;
    private int portNumber;
    private long begin;
    private long end;
    private int totalKeys;
    public CacheReads(String host,int port,int numberOfKeys) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
        populateDatabase();
    }

    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++) {
            jedis.set(String.valueOf(i) , "hello" + i); //Populating the database with multiple keys
        }
    }

    public long JedisTest() {
        Jedis jedisInstance = new Jedis(hostName,portNumber);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            jedisInstance.get(String.valueOf(i)); // Initial Reads , reads directly from the server
            for(int j = i ; j >= 0 ; j--){
                jedisInstance.get(String.valueOf(j)); // No Caching available reads from the server creates delays
            }
        }
        end = Calendar.getInstance().getTimeInMillis();
        jedisInstance.close();
        return (end-begin);
    }

    public long CacheJedisTest(){
        CacheJedis cacheJedisInstance = new CacheJedis(hostName,portNumber);
        CacheConfig cacheConfig = CacheConfig.Builder.newInstance().maxSize(totalKeys).build();
        cacheJedisInstance.enableCaching(cacheConfig);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys  ; i++){
            cacheJedisInstance.get(String.valueOf(i)); // Initial Reads , reads directly from the server
            for(int j = i ; j >= 0 ; j--){
                cacheJedisInstance.get(String.valueOf(j)); // Cached Reads , reads from the local cache
            }
        }
        end = Calendar.getInstance().getTimeInMillis();
        cacheJedisInstance.close();
        return (end-begin);
    }
}
