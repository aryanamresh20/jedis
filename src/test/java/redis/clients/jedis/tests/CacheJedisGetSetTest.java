package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;
import static org.junit.Assert.assertEquals;

public class CacheJedisGetSetTest {

    @Test
    public void testCacheJedisGetSet() throws InterruptedException {

        CacheJedis cacheJedis = new CacheJedis(); //Cached Jedis Object
        Jedis jedis = new Jedis(); //Simple Jedis Object
        cacheJedis.set("foo","bar");
        assertEquals(0,cacheJedis.getCacheSize()); //Nothing in the cache at starting
        cacheJedis.get("foo");
        assertEquals(1,cacheJedis.getCacheSize()); //Key foo cached in the cache
        jedis.set("foo","new foo"); //Another Client changes the value of foo to generate invalidation message
        Thread thread = Thread.currentThread();
        //Pausing the main thread to let cache invalidate the message
        try{
            thread.sleep(200);
        }
        catch(InterruptedException e) {
        }
        assertEquals(0,cacheJedis.getCacheSize()); //Key foo invalidated from the cache
        cacheJedis.close();
        jedis.close();

    }
}

