package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.*;

public class ReadsHgetAll {
    private final String hostName;
    private final int portNumber;
    private long begin;
    private long end;
    private final long totalKeys;
    public ReadsHgetAll(String host, int port, long numberOfKeys) {
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
        for(int i = 0 ; i < totalKeys ; i++){
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
        CachedJedis cachedJedisInstance = new CachedJedis(hostName,portNumber);
        JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder()
                .maxCacheSize(totalKeys*2)
                .expireAfterAccess(1000)
                .expireAfterWrite(1000)
                .build();
        cachedJedisInstance.setupCaching(jedisCacheConfig);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            // Initial Reads ,these keys reads directly from the server
            cachedJedisInstance.hgetAll("key"+i);
            for(int j = i ; j >= 0 ; j--){
                // No Caching available these keys also reads from the server creates delays
                cachedJedisInstance.hgetAll("key"+j);
            }
        }
        end = Calendar.getInstance().getTimeInMillis();
        cachedJedisInstance.close();
        return (end-begin);
    }
}
