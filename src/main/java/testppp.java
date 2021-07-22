import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

public class testppp {

    public  static  void main(String args[])
    {
        JedisPool pool = new JedisPool();
        Jedis jedis = pool.getResource();
        CachedJedisPool cachedJedisPool = new CachedJedisPool();
        CachedJedisPool cachedJedisPool1 = new CachedJedisPool();
        cachedJedisPool.startCaching();
        cachedJedisPool1.startCaching();
        CachedJedis cachedJedis = cachedJedisPool.getResource();
        CachedJedis cachedJedis1 = cachedJedisPool.getResource();
        cachedJedis.set("hello","world");

    }

}
