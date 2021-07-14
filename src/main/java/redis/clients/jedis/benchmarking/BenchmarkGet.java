package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class BenchmarkGet {

    private final String hostName;
    private final int portNumber;
    private final long totalKeys;
    private final long expireTimeAccess;
    private final long expireTimeWrite;
    private final long warmCachePercentage;

    public BenchmarkGet(String host, int port, long numberOfKeys, long expireAfterAccess,
                        long expireAfterWrite, long warmCachePercentage) {
        this.hostName = host;
        this.portNumber = port;
        this.totalKeys = numberOfKeys;
        this.expireTimeAccess = expireAfterAccess;
        this.expireTimeWrite = expireAfterWrite;
        this.warmCachePercentage = warmCachePercentage;
    }

    public long getJedisRunningTime() {
        try (Jedis jedisInstance = new Jedis(hostName, portNumber)) {
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                // No Caching available reads from the server creates delays
                jedisInstance.get(KEY_PREFIX + i);
            }
            long end = System.currentTimeMillis();
            jedisInstance.quit();
            return (end - begin);
        }
    }

    public long getCachedJedisRunningTime(boolean warmCache) {
        try (CachedJedis cachedJedisInstance = new CachedJedis(hostName, portNumber)) {
            JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder()
                .maxCacheSize(totalKeys * 2)
                .expireAfterAccess(expireTimeAccess)
                .expireAfterWrite(expireTimeWrite)
                .build();
            //cachedJedisInstance.setupCaching(jedisCacheConfig);
            if (warmCache) {
                BenchmarkingUtil.warmCache(cachedJedisInstance, warmCachePercentage, totalKeys, false);
            }
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                // Cache reads , reads from the cache
                cachedJedisInstance.get(KEY_PREFIX + i);
            }
            long end = System.currentTimeMillis();
            cachedJedisInstance.quit();
            return (end - begin);
        }
    }
}
