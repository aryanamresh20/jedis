package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CachedJedis;

/**
 * User: manas.joshi
 * Date: 06-07-2021
 * Time: 2:18 PM
 */

public class BenchmarkingCachedJedis extends CachedJedis {

    public BenchmarkingCachedJedis(String host, int port) {
        super(host, port);
    }

    @Override
    protected void invalidateCache(String key) {
        super.invalidateCache(key);
        // add monitoring here

    }
}