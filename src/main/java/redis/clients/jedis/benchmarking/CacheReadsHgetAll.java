package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.*;

public class CacheReadsHgetAll {
    private final String hostName;
    private final int portNumber;
    private final long totalKeys;
    public CacheReadsHgetAll(String host, int port, long numberOfKeys) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
        populateDatabase();
    }

    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++) {
            Map<String , String> map = new HashMap<>();
            map.put("hello"+i,"world"+i);
            map.put("hello1"+i,"world1"+i);
            map.put("hello2"+i,"world2"+i);
            //Populating the database with multiple hash
            jedis.hset("key"+i,map);
        }
    }

    public long JedisTest() {
        Jedis jedisInstance = new Jedis(hostName,portNumber);
        long begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            // Initial Reads ,these keys reads directly from the server
            jedisInstance.hgetAll("key"+i);
            for(int j = i ; j >= 0 ; j--){
                // No Caching available these keys also reads from the server creates delays
                jedisInstance.hgetAll("key"+j);
            }
        }
        long end = Calendar.getInstance().getTimeInMillis();
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
        long begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            // Initial Reads ,these keys reads directly from the server
            cachedJedisInstance.hgetAll("key"+i);
            for(int j = i ; j >= 0 ; j--){
                // No Caching available these keys also reads from the server creates delays
                cachedJedisInstance.hgetAll("key"+j);
            }
        }
        long end = Calendar.getInstance().getTimeInMillis();
        cachedJedisInstance.close();
        return (end-begin);
    }
}
