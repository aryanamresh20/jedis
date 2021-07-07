package redis.clients.jedis.benchmarking;

import javafx.util.Pair;
import redis.clients.jedis.CachedJedis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * User: manas.joshi
 * Date: 06-07-2021
 * Time: 2:18 PM
 */

public class BenchmarkingCachedJedis extends CachedJedis {

    public final ConcurrentHashMap<String , Long>  staleTime = new ConcurrentHashMap<>();
    public final List<Long> staleTimes = new ArrayList<>();
    public BenchmarkingCachedJedis(String host, int port) {
        super(host, port);
    }

    @Override
    protected void invalidateCache(String key) {
        super.invalidateCache(key);
        if(staleTime.get(key)!=null){
            staleTimes.add(System.currentTimeMillis()-staleTime.get(key));
            staleTime.remove(key);
        }
    }

}