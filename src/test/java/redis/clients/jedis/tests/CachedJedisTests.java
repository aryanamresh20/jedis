package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCacheConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CachedJedisTests {


    @Test
    public void CachedJedisGetTest() throws InterruptedException {

        CachedJedis cachedJedis = new CachedJedis(); //Cached Jedis Object
        //cachedJedis.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        Jedis jedis = new Jedis(); //Simple Jedis Object
        cachedJedis.set("foo","bar");
        assertEquals(0, cachedJedis.getCacheSize()); //Nothing in the cache at starting
        cachedJedis.get("foo");
        assertEquals(1, cachedJedis.getCacheSize()); //Key foo cached in the cache

        jedis.set("foo","new foo"); //Another Client changes the value of foo to generate invalidation message
        //Pausing the main thread to let cache invalidate the message

        Thread.sleep(20);

        assertEquals(0, cachedJedis.getCacheSize()); //Key foo invalidated from the cache

        assertEquals(cachedJedis.get("fooNotExist"),jedis.get("fooNotExist"));   // Both returns null , both get from server
        assertEquals(cachedJedis.get("fooNotExist"),jedis.get("fooNotExist"));   // Both returns null ,cache jedis gets from cache

        cachedJedis.close();
        jedis.close();

    }

    @Test
    public void CachedJedisMgetTest() throws InterruptedException{
        CachedJedis cachedJedis = new CachedJedis(); //Cache Jedis Object
       // cachedJedis.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        Jedis jedis = new Jedis(); //Jedis Object
        jedis.mset("foo","bar","foo1","bar1","foo2","bar2"); //Populating database with three keys
        List<String> jedisResponse = jedis.mget("foo","foo1","foo2");
        List<String> cacheJedisResponse = cachedJedis.mget("foo","foo1","foo2");
        assertEquals(3, cachedJedis.getCacheSize()); //All the three keys not present were cached
        assertEquals(jedisResponse,cacheJedisResponse); //response from both the objects should be same

        jedis.set("foo1","newbar"); //sends an invalidation message
        Thread.sleep(10); //waiting for the message to receive and cache invalidation
        assertEquals(2, cachedJedis.getCacheSize()); //key "happy" invalidated cache size decreased

        jedisResponse = jedis.mget("foo","foo1","foo2");
        cacheJedisResponse = cachedJedis.mget("foo","foo1","foo2");
        assertEquals(jedisResponse,cacheJedisResponse); //Repeating the same process one key from server other two keys from cache

        assertEquals(cachedJedis.mget("fooNotExist","foo"),jedis.mget("fooNotExist","foo"));   // for fooNotExist , both get from server , return null
        assertEquals(cachedJedis.mget("fooNotExist","foo"),jedis.mget("fooNotExist","foo"));   // for fooNotExist ,cache jedis gets from cache , returns null
        cachedJedis.close();
        jedis.close();
    }

    @Test
    public void CachedJedisHgetAllTest() throws InterruptedException {

        Map<String , String> map = new HashMap<>();
        map.put("hello","world");
        map.put("hello1","world1");
        map.put("hello2","world2");

        CachedJedis cachedJedis = new CachedJedis(); //Cache Jedis Object
       // cachedJedis.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        Jedis jedis = new Jedis(); //Jedis Object
        jedis.hset("keyhash" , map); //Populating database with a hashMap
        Map<String , String> jedisResponse = jedis.hgetAll("keyhash");
        Map<String , String> cacheJedisResponse = cachedJedis.hgetAll("keyhash");
        assertEquals(1 , cachedJedis.getCacheSize()); //hashMap was cached
        assertEquals(jedisResponse , cacheJedisResponse); //response from both the objects should be same

        cacheJedisResponse = cachedJedis.hgetAll("keyhash");
        assertEquals(jedisResponse , cacheJedisResponse); //same response from the cache

        jedis.hset("keyhash" , "hello1" , "new world1"); //sends an invalidation message
        Thread.sleep(10); //waiting for the message to receive and cache invalidation
        assertEquals(0 , cachedJedis.getCacheSize()); //hash map was invalidated

        jedisResponse = jedis.hgetAll("keyhash");
        cacheJedisResponse = cachedJedis.hgetAll("keyhash");
        assertEquals(jedisResponse , cacheJedisResponse); //Repeating the same process one key from server other two keys from cache

        assertEquals(cachedJedis.hgetAll("fooNotExist"),jedis.hgetAll("fooNotExist"));   // Both returns null , both get from server
        assertEquals(cachedJedis.hgetAll("fooNotExist"),jedis.hgetAll("fooNotExist"));   // Both returns null ,cache jedis gets from cache

        cachedJedis.close();
        jedis.close();

    }

    @Test
    public void CachedJedisQuitTest() {
        CachedJedis cachedJedis = new CachedJedis();
        cachedJedis.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        assertEquals("OK", cachedJedis.quit());
    }

    @Test
    public void CachedJedisCloseTest() {
        CachedJedis cachedJedis = new CachedJedis();
        cachedJedis.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        assertTrue(cachedJedis.isConnected());
        cachedJedis.close();
        assertFalse(cachedJedis.isConnected());
    }
}
