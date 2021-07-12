package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.JedisCacheConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class BenchmarkingInvalidationMessages extends CachedJedis {

    private final long totalKeys;
    private final List<Long> setTime = new ArrayList<>();
    private final List<Long> invalidationTime = new ArrayList<>();
    private final List<Long> invalidationLatency = new ArrayList<>();
    public BenchmarkingInvalidationMessages(String host , int port , long totalKeys){
        super(host, port);
        this.setupCaching(JedisCacheConfig.Builder.newBuilder().maxCacheSize(totalKeys*2).build());
        this.totalKeys = totalKeys;
        getInCache();
        InvalidateFromCache();
        this.close();
    }

    private void getInCache(){
        for(int i=0 ; i < totalKeys ; i++){
            this.get(KEY_PREFIX + i);
        }
    }

    private  void InvalidateFromCache(){
        for(int i=0 ; i < totalKeys ; i++){
            this.set(KEY_PREFIX + i, "invalidate");
            setTime.add(System.nanoTime());
        }
    }
    @Override
    protected void invalidateCache(String key) {
        super.invalidateCache(key);
        invalidationTime.add(System.nanoTime());
    }

    public void getInvalidationLatency(){
        for(int i=0 ; i < totalKeys ; i++){
            invalidationLatency.add(invalidationTime.get(i)-setTime.get(i));
        }
        printPValues(invalidationLatency);
    }

    private void printPValues(List<Long> latencies) {
        Collections.sort(latencies);
        if(latencies.size() == 0) {
            return;
        }
        long p50Time = latencies.get((int) (latencies.size() * 0.5));
        long p75Time = latencies.get((int) (latencies.size() * 0.75));
        long p90Time = latencies.get((int) (latencies.size() * 0.90));
        long p95Time = latencies.get((int) (latencies.size() * 0.95));
        long p97Time = latencies.get((int) (latencies.size() * 0.97));
        long p99Time = latencies.get((int) (latencies.size() * 0.99));
        long p995Time = latencies.get((int) (latencies.size() * 0.995));
        long p997Time = latencies.get((int) (latencies.size() * 0.997));
        long p999Time = latencies.get((int) (latencies.size() * 0.999));
        long p9995Time = latencies.get((int) (latencies.size() * 0.9995));
        long p100Time = latencies.get((latencies.size() - 1));
        System.out.println("P50 " + p50Time/1000000.0);
        System.out.println("P75 " + p75Time/1000000.0);
        System.out.println("P90 " + p90Time/1000000.0);
        System.out.println("P95 " + p95Time/1000000.0);
        System.out.println("P97 " + p97Time/1000000.0);
        System.out.println("P99 " + p99Time/1000000.0);
        System.out.println("P99.5 " + p995Time/1000000.0);
        System.out.println("P99.7 " + p997Time/1000000.0);
        System.out.println("P99.9 " + p999Time/1000000.0);
        System.out.println("P99.95 " + p9995Time/1000000.0);
        System.out.println("P100 " + p100Time/1000000.0);
        double average = (latencies.stream().mapToDouble(d->d).average()).orElse(-1);
        System.out.println("Average latency " + average/1000000.0);
    }

}
