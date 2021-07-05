package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCacheConfig;

import static org.junit.Assert.assertEquals;

public class CachedJedisGetSetTest {

    @Test
    public void testCacheJedisGetSet() throws InterruptedException {

        CachedJedis cachedJedis = new CachedJedis(); //Cached Jedis Object
        cachedJedis.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        Jedis jedis = new Jedis(); //Simple Jedis Object
        cachedJedis.set("foo","bar");
        assertEquals(0, cachedJedis.getCacheSize()); //Nothing in the cache at starting
        cachedJedis.get("foo");
        assertEquals(1, cachedJedis.getCacheSize()); //Key foo cached in the cache
        jedis.set("foo","new foo"); //Another Client changes the value of foo to generate invalidation message
        Thread thread = Thread.currentThread();
        //Pausing the main thread to let cache invalidate the message
        try{
            thread.sleep(200);
        }
        catch(InterruptedException e) {
        }
        assertEquals(0, cachedJedis.getCacheSize()); //Key foo invalidated from the cache
        cachedJedis.close();
        jedis.close();

    }
}

