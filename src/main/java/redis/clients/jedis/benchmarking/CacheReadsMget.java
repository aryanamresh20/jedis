package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class CacheReadsMget {

    private final String hostName;
    private final int portNumber;
    private final long totalKeys;

    public CacheReadsMget(String host, int port, long numberOfKeys) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
    }

    public long JedisTest() {
        try (Jedis jedisInstance = new Jedis(hostName, portNumber)) {
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                List<String> mgetInstance = new ArrayList<>();
                // Initial Reads ,these keys reads directly from the server
                mgetInstance.add(KEY_PREFIX + i);
                for (int j = i; j >= 0; j--) {
                    // No Caching available these keys also reads from the server creates delays
                    mgetInstance.add(KEY_PREFIX + j);
                }
                String[] mgetInstanceArray = new String[mgetInstance.size()];
                mgetInstanceArray = mgetInstance.toArray(mgetInstanceArray);
                jedisInstance.mget(mgetInstanceArray);
            }
            long end = System.currentTimeMillis();
            jedisInstance.quit();
            return (end - begin);
        }
    }

    public long CacheJedisTest(){
        try (CachedJedis cachedJedisInstance = new CachedJedis(hostName, portNumber)) {
            JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder()
                .maxCacheSize(totalKeys * 2)
                .expireAfterAccess(1000)
                .expireAfterWrite(1000)
                .build();
            cachedJedisInstance.setupCaching(jedisCacheConfig);
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                List<String> mgetInstance = new ArrayList<>();
                // Initial Reads ,these keys reads directly from the server
                mgetInstance.add(KEY_PREFIX + i);
                for (int j = i; j >= 0; j--) {
                    //Caching available these keys reads from the local Cache
                    mgetInstance.add(KEY_PREFIX + j);
                }
                String[] mgetInstanceArray = new String[mgetInstance.size()];
                mgetInstanceArray = mgetInstance.toArray(mgetInstanceArray);
                cachedJedisInstance.mget(mgetInstanceArray);
            }
            long end = System.currentTimeMillis();
            cachedJedisInstance.quit();
            return (end - begin);
        }
    }
}
