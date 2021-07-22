package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.util.Pool;

@Deprecated
public class CachedJedisPoolAbstract extends Pool<CachedJedis> {

    @Deprecated
    public CachedJedisPoolAbstract() {
        super();
    }

    public CachedJedisPoolAbstract(GenericObjectPoolConfig<CachedJedis> poolConfig,
                             PooledObjectFactory<CachedJedis> factory) {
        super(poolConfig, factory);
    }
}
