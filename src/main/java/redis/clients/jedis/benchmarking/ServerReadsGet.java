package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCacheConfig;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class ServerReadsGet {

    private final String hostName;
    private final int portNumber;
    private final long totalKeys;

    public ServerReadsGet(String host, int port, long numberOfKeys) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
    }

    public long JedisTest() {
        try (Jedis jedisInstance = new Jedis(hostName, portNumber)) {
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                //Single reads , reads directly from the server
                jedisInstance.get(KEY_PREFIX + i);
            }
            long end = System.currentTimeMillis();
            jedisInstance.quit();
            return (end - begin);
        }
    }

    public long CacheJedisTest(){
        try (CachedJedis cachedJedisInstance = new CachedJedis(hostName, portNumber)) {
            cachedJedisInstance.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
            long begin = System.currentTimeMillis();
            for (int i = 0; i < totalKeys; i++) {
                //Single reads , reads directly from the server , keys not cached
                cachedJedisInstance.get(KEY_PREFIX + i);
            }
            long end = System.currentTimeMillis();
            cachedJedisInstance.quit();
            return (end - begin);
        }
    }
}
