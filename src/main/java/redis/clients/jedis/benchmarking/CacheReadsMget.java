package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class CacheReadsMget {

    private final String hostName;
    private final int portNumber;
    private final long totalKeys;
    private final long expireTimeAccess;
    private final long expireTimeWrite;

    public CacheReadsMget(String host, int port, long numberOfKeys, long expireAfterAccess, long expireAfterWrite) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
        expireTimeAccess = expireAfterAccess;
        expireTimeWrite = expireAfterWrite;
    }

    public long JedisTest() {
        try (Jedis jedisInstance = new Jedis(hostName, portNumber)) {
            long finalDuration = 0;
            Random rand = new Random();
            for (int i = 0; i < totalKeys; i++) {
                List<String> mgetInstance = new ArrayList<>();
                // Initial Reads ,these keys reads directly from the server
                mgetInstance.add(KEY_PREFIX + i);
                for (int j = 0; j < rand.nextInt((int) totalKeys) ; j++) {
                    // No Caching available these keys also reads from the server creates delays
                    mgetInstance.add(KEY_PREFIX + rand.nextInt((int) totalKeys));
                }
                String[] mgetInstanceArray = new String[mgetInstance.size()];
                mgetInstanceArray = mgetInstance.toArray(mgetInstanceArray);
                long begin = System.currentTimeMillis();
                jedisInstance.mget(mgetInstanceArray);
                long end = System.currentTimeMillis();
                finalDuration += (end-begin);
            }
            jedisInstance.quit();
            return finalDuration;
        }
    }

    public long CacheJedisTest(){
        try (CachedJedis cachedJedisInstance = new CachedJedis(hostName, portNumber)) {
            JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder()
                .maxCacheSize(totalKeys * 2)
                .expireAfterAccess(expireTimeAccess)
                .expireAfterWrite(expireTimeWrite)
                .build();
            cachedJedisInstance.setupCaching(jedisCacheConfig);
            Random rand = new Random();
            long finalDuration = 0;
            for (int i = 0; i < totalKeys; i++) {
                List<String> mgetInstance = new ArrayList<>();
                // Initial Reads ,these keys reads directly from the server
                mgetInstance.add(KEY_PREFIX + i);
                for (int j =0 ; j < rand.nextInt((int) totalKeys); j++) {
                    //Caching available these keys reads from the local Cache
                    mgetInstance.add(KEY_PREFIX + rand.nextInt((int) totalKeys));
                }
                String[] mgetInstanceArray = new String[mgetInstance.size()];
                mgetInstanceArray = mgetInstance.toArray(mgetInstanceArray);
                long begin = System.currentTimeMillis();
                cachedJedisInstance.mget(mgetInstanceArray);
                long end = System.currentTimeMillis();
                finalDuration += (end-begin);
            }
            cachedJedisInstance.quit();
            return finalDuration;
        }
    }
}
