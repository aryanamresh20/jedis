package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CacheJedisHgetAllTest {

    @Test
    public void testCacheJedisHget() throws InterruptedException {

        Map<String , String> map = new HashMap<>();
        map.put("hello","world");
        map.put("hello1","world1");
        map.put("hello2","world2");

        CacheJedis cacheJedis = new CacheJedis(); //Cache Jedis Object
        cacheJedis.enableCaching();
        Jedis jedis = new Jedis(); //Jedis Object
        jedis.hset("key" , map); //Populating database with a hashMap
        Map<String , String> jedisResponse = jedis.hgetAll("key");
        Map<String , String> cacheJedisResponse = cacheJedis.hgetAll("key");
        assertEquals(1 , cacheJedis.getCacheSize()); //hashMap was cached
        assertEquals(jedisResponse , cacheJedisResponse); //response from both the objects should be same
        cacheJedisResponse = cacheJedis.hgetAll("key");
        assertEquals(jedisResponse , cacheJedisResponse); //same response from the cache
        jedis.hset("key" , "hello1" , "new world1"); //sends an invalidation message
        Thread thread = Thread.currentThread();
        thread.sleep(10); //waiting for the message to receive and cache invalidation
        assertEquals(0 , cacheJedis.getCacheSize()); //hash map was invalidated
        jedisResponse = jedis.hgetAll("key");
        cacheJedisResponse = cacheJedis.hgetAll("key");
        assertEquals(jedisResponse , cacheJedisResponse); //Repeating the same process one key from server other two keys from cache
        cacheJedis.close();
        jedis.close();

    }
}
