package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;

import java.util.*;

public class ReadsHget {
    private String hostName;
    private int portNumber;
    private long begin;
    private long end;
    private int totalKeys;
    public ReadsHget(String host,int port,int numberOfKeys) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
        populateDatabase();
    }

    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        Map<String , String> map = new HashMap<>();
        map.put("hello","world");
        map.put("hello1","world1");
        map.put("hello2","world2");
        for(int i = 0 ; i < totalKeys ; i++) {
            //Populating the database with multiple hash
            jedis.hset("key"+i,map);
        }
    }

    public long JedisTest() {
        Jedis jedisInstance = new Jedis(hostName,portNumber);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < 100 ; i++){
            // Initial Reads ,these keys reads directly from the server
            jedisInstance.hgetAll("key"+i);
            for(int j = i ; j >= 0 ; j--){
                // No Caching available these keys also reads from the server creates delays
                jedisInstance.hgetAll("key"+j);
            }
        }
        end = Calendar.getInstance().getTimeInMillis();
        jedisInstance.close();
        return (end-begin);
    }

    public long CacheJedisTest(){
        CacheJedis cacheJedisInstance = new CacheJedis(hostName,portNumber);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < 100 ; i++){
            // Initial Reads ,these keys reads directly from the server
            cacheJedisInstance.hgetAll("key"+i);
            for(int j = i ; j >= 0 ; j--){
                // No Caching available these keys also reads from the server creates delays
                cacheJedisInstance.hgetAll("key"+j);
            }
        }
        end = Calendar.getInstance().getTimeInMillis();
        cacheJedisInstance.close();
        return (end-begin);
    }
}
