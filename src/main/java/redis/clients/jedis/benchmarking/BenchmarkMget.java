package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class BenchmarkMget {

    private final String hostName;
    private final int portNumber;
    private final long totalKeys;
    private final long expireTimeAccess;
    private final long expireTimeWrite;
    private final long warmCachePercentage;

    public BenchmarkMget(String host, int port, long numberOfKeys, long expireAfterAccess,
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
            long finalDuration = 0;
            for (int i = 0; i < totalKeys; i++) {
                String[] sampleKeys = getSampleKeys();
                long begin = System.currentTimeMillis();
                jedisInstance.mget(sampleKeys);
                long end = System.currentTimeMillis();
                finalDuration += (end - begin);
            }
            jedisInstance.quit();
            return finalDuration;
        }
    }

    public long getCachedJedisRunningTime(boolean warmCache) {
        try (CachedJedis cachedJedisInstance = new CachedJedis(hostName, portNumber)) {
            JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder()
                .maxCacheSize(totalKeys * 2)
                .expireAfterAccess(expireTimeAccess)
                .expireAfterWrite(expireTimeWrite)
                .build();
            cachedJedisInstance.setupCaching(jedisCacheConfig);
            if (warmCache) {
                BenchmarkingUtil.warmCache(cachedJedisInstance, warmCachePercentage, totalKeys, false);
            }
            long finalDuration = 0;
            for (int i = 0; i < totalKeys; i++) {
                String[] sampleKeys = getSampleKeys();
                long begin = System.currentTimeMillis();
                cachedJedisInstance.mget(sampleKeys);
                long end = System.currentTimeMillis();
                finalDuration += (end - begin);
            }
            cachedJedisInstance.quit();
            return finalDuration;
        }
    }

    private String[] getSampleKeys() {
        List<String> mgetInstance = new ArrayList<>();
        long numberOfKeys = ThreadLocalRandom.current().nextLong(1, Math.max(100,totalKeys/500));
        for (int j = 0; j < numberOfKeys; j++) {
            mgetInstance.add(KEY_PREFIX + ThreadLocalRandom.current().nextLong(totalKeys));
        }
        return mgetInstance.toArray(new String[0]);
    }
}
