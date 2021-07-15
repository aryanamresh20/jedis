package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CachedJedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * User: manas.joshi
 * Date: 06-07-2021
 * Time: 2:18 PM
 */

public class BenchmarkingCachedJedis extends CachedJedis {

    public static final String KEY_PREFIX = "benchmarking:test:";
    public static final String HASH_KEY_PREFIX = "benchmarking:test:hash:";

    private static final Object DUMMY = new Object();
    private final List<Long> serverGetLatencies = new ArrayList<>();
    private final List<Long> putInCacheLatencies = new ArrayList<>();

    public BenchmarkingCachedJedis(String host, int port) {
        super(host, port);
    }


    public Boolean boolGet(String key) {
        if (isCachingEnabled()) {
            if (super.getFromCache(key) != null) {
                return true;
            } else {
                long start = System.nanoTime();
                String value = super.get(key);
                long end = System.nanoTime();
                serverGetLatencies.add(end - start);
                start = System.nanoTime();
                if (value != null) {
                    putInCache(key, value);
                } else {
                    putInCache(key, DUMMY);
                }
                end = System.nanoTime();
                putInCacheLatencies.add(end - start);
                return false;
            }
        } else {
            long start = System.nanoTime();
            super.get(key);
            long end = System.nanoTime();
            serverGetLatencies.add(end-start);
            return false;
        }
    }

    public List<Long> getServerGetLatencies(){
        return serverGetLatencies;
    }
    public List<Long> getPutInCacheLatencies(){
        return putInCacheLatencies;
    }
}