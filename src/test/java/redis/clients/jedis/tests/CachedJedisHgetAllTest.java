package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCacheConfig;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CachedJedisHgetAllTest {

    @Test
    public void testCacheJedisHget() throws InterruptedException {

        Map<String , String> map = new HashMap<>();
        map.put("hello","world");
        map.put("hello1","world1");
        map.put("hello2","world2");

        CachedJedis cachedJedis = new CachedJedis(); //Cache Jedis Object
        cachedJedis.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        Jedis jedis = new Jedis(); //Jedis Object
        jedis.hset("keyhash" , map); //Populating database with a hashMap
        Map<String , String> jedisResponse = jedis.hgetAll("keyhash");
        Map<String , String> cacheJedisResponse = cachedJedis.hgetAll("keyhash");
        assertEquals(1 , cachedJedis.getCacheSize()); //hashMap was cached
        assertEquals(jedisResponse , cacheJedisResponse); //response from both the objects should be same
        cacheJedisResponse = cachedJedis.hgetAll("keyhash");
        assertEquals(jedisResponse , cacheJedisResponse); //same response from the cache
        jedis.hset("keyhash" , "hello1" , "new world1"); //sends an invalidation message
        Thread thread = Thread.currentThread();
        thread.sleep(10); //waiting for the message to receive and cache invalidation
        assertEquals(0 , cachedJedis.getCacheSize()); //hash map was invalidated
        jedisResponse = jedis.hgetAll("keyhash");
        cacheJedisResponse = cachedJedis.hgetAll("keyhash");
        assertEquals(jedisResponse , cacheJedisResponse); //Repeating the same process one key from server other two keys from cache
        cachedJedis.close();
        jedis.close();

    }
}
