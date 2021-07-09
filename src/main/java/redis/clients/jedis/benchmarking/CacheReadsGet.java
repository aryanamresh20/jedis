package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.Calendar;

public class CacheReadsGet {

    private final String hostName;
    private final int portNumber;
    private final long totalKeys;
    public CacheReadsGet(String host, int port, long numberOfKeys) {
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
        long begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            jedisInstance.get(String.valueOf(i)); // Initial Reads , reads directly from the server
            for(int j = i ; j >= 0 ; j--){
                jedisInstance.get(String.valueOf(j)); // No Caching available reads from the server creates delays
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
        for(int i = 0 ; i < totalKeys  ; i++){
            cachedJedisInstance.get(String.valueOf(i)); // Initial Reads , reads directly from the server
            for(int j = i ; j >= 0 ; j--){
                cachedJedisInstance.get(String.valueOf(j)); // Cached Reads , reads from the local cache
            }
        }
        long end = Calendar.getInstance().getTimeInMillis();
        cachedJedisInstance.close();
        return (end-begin);
    }
}
