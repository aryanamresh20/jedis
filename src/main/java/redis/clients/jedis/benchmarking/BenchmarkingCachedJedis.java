package redis.clients.jedis.benchmarking;

import com.google.common.annotations.VisibleForTesting;
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

    private static final Object DUMMY = new Object();
    public final HashMap<String , Long> staleTime = new HashMap<>();
    public final List<Long> staleTimeLatencies = new ArrayList<>();
    public BenchmarkingCachedJedis(String host, int port) {
        super(host, port);
    }
    public final List<Long> serverGetLatencies = new ArrayList<>();
    public List<Long> putInCacheLatencies = new ArrayList<>();

    @Override
    protected void invalidateCache(String key) {
        super.invalidateCache(key);
        if(staleTime.get(key)!=null){
            staleTimeLatencies.add(System.nanoTime()-staleTime.get(key));
            staleTime.remove(key);
        }
    }
    @VisibleForTesting
    public Boolean boolGet(String key) {
        if(getIfCachingEnabled()) {
            if (super.getFromCache(key) != null) {
                return true;
            } else {
                long start = System.nanoTime();
                String value = super.get(key);
                long end = System.nanoTime();
                serverGetLatencies.add(end-start);
                start = System.nanoTime();
                if (value != null) {
                    putInCache(key , value);
                } else {
                    putInCache(key , DUMMY);
                }
                end = System.nanoTime();
                putInCacheLatencies.add(end-start);
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

}