package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class CacheReadsGet {

    private final String hostName;
    private final int portNumber;
    private final long totalKeys;
    private final long expireTimeAccess;
    private final long expireTimeWrite;

    public CacheReadsGet(String host, int port, long numberOfKeys, long expireAfterAccess, long expireAfterWrite) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
        expireTimeAccess = expireAfterAccess;
        expireTimeWrite = expireAfterWrite;
    }

    public long JedisTest() {
        try (Jedis jedisInstance = new Jedis(hostName, portNumber)) {
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                // Initial Reads , reads directly from the server
                jedisInstance.get(KEY_PREFIX + i);
                for (int j = i; j >= 0; j--) {
                    // No Caching available reads from the server creates delays
                    jedisInstance.get(KEY_PREFIX + j);
                }
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
                .expireAfterAccess(expireTimeAccess)
                .expireAfterWrite(expireTimeWrite)
                .build();
            cachedJedisInstance.setupCaching(jedisCacheConfig);
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                // Initial Reads , reads directly from the server
                cachedJedisInstance.get(KEY_PREFIX + i);
                for (int j = i; j >= 0; j--) {
                    // Cached Reads , reads from the local cache
                    cachedJedisInstance.get(KEY_PREFIX + j);
                }
            }
            long end = System.currentTimeMillis();
            cachedJedisInstance.quit();
            return (end - begin);
        }
    }
}
