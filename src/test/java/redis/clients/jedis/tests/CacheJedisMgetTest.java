package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;
import static org.junit.Assert.assertEquals;

import java.util.List;

public class CacheJedisMgetTest {

    @Test
    public void testCacheJedisMget() throws InterruptedException{
        CacheJedis cacheJedis = new CacheJedis(); //Cache Jedis Object
        cacheJedis.enableCaching();
        Jedis jedis = new Jedis(); //Jedis Object
        jedis.mset("foo","bar","happy","birthday","hello","world"); //Populating database with three keys
        List<String> jedisResponse = jedis.mget("foo","happy","hello");
        List<String> cacheJedisResponse = cacheJedis.mget("foo","happy","hello");
        assertEquals(3,cacheJedis.getCacheSize()); //All the three keys not present were cached
        assertEquals(jedisResponse,cacheJedisResponse); //response from both the objects should be same
        jedis.set("happy","new"); //sends an invalidation message
        Thread thread = Thread.currentThread();
        thread.sleep(10); //waiting for the message to receive and cache invalidation
        assertEquals(2,cacheJedis.getCacheSize()); //key "happy" invalidated cache size decreased
        jedisResponse = jedis.mget("foo","happy","hello");
        cacheJedisResponse = cacheJedis.mget("foo","happy","hello");
        assertEquals(jedisResponse,cacheJedisResponse); //Repeating the same process one key from server other two keys from cache
        cacheJedis.close();
        jedis.close();
    }
}
