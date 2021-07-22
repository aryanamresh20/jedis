package redis.clients.jedis.benchmarking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.CachedJedisPool;
import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

public class CachedJedisLatencies {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachedJedisLatencies.class);

    private final String hostName;
    private final int portNumber;
    private final long writePercentage;
    private final long totalClients ;
    private final long totalKeys;
    private final long totalOperations;
    private final long sigmaOperationTime;
    private final long meanOperationTime;
    private final long expireAfterAccessMillis;
    private final long expireAfterWriteMillis;
    private final long messageSize;
    private final long warmCacheIterations;
    private final long interval;
    private final long readFromGroup;
    private final boolean enableCaching;

    private final AtomicLong staleCount = new AtomicLong();
    private final AtomicLong cacheHit = new AtomicLong();
    private final AtomicLong totalGet = new AtomicLong();
    private CachedJedisPool cachedJedisPool = new CachedJedisPool();
    private final List<Long> operationsTimeLatencies = new CopyOnWriteArrayList<>();
    private final List<Long> serverLatencies = new CopyOnWriteArrayList<>();
    private final List<Long> cacheLatencies = new CopyOnWriteArrayList<>();
    //To keep track of the last client setting the key
    private final ConcurrentHashMap<String , HashSet<String> > checkStale = new ConcurrentHashMap<>();

    public CachedJedisLatencies(String hostName, int portNumber, long writePercentage, long totalClients,
                                long totalKeys, long totalOperations, long sigmaOperationTime, long meanOperationTime,
                                long expireAfterAccessMillis, long expireAfterWriteMillis, long messageSize,
                                long warmCacheIterations, long readFromGroup, boolean enableCaching) {
        cachedJedisPool.startCaching();
        this.hostName = hostName;
        this.portNumber = portNumber;
        this.writePercentage = writePercentage;
        this.totalClients = totalClients;
        this.totalKeys = totalKeys;
        this.totalOperations = totalOperations;
        this.sigmaOperationTime = sigmaOperationTime;
        this.meanOperationTime = meanOperationTime;
        this.expireAfterAccessMillis = expireAfterAccessMillis;
        this.expireAfterWriteMillis = expireAfterWriteMillis;
        this.messageSize = messageSize;
        this.warmCacheIterations = warmCacheIterations;
        this.interval = totalKeys/totalClients;
        this.readFromGroup = readFromGroup;
        this.enableCaching = enableCaching;
    }

    //Starting multiple cacheJedis instances on multiple threads
    public void beginBenchmark() {
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < totalClients; i++) {
            Thread thread = new Thread(createBenchmarkingRunnable(i));
            threadList.add(thread);
            thread.setName("CLIENT_THREAD_" + i);
        }
        for (Thread thread : threadList) {
            thread.start();
        }
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOGGER.error("[FAILED_BENCHMARK] Thread {} failed with error", thread.getName(), e);
            }
        }
    }

    public long getStaleCount() {
        return staleCount.get();
    }

    public long getCacheHit() {
        return cacheHit.get();
    }

    public long getCacheMiss() {
        return totalGet.get() - cacheHit.get();
    }

    public void getOverallLatencies() {
        System.out.println("Overall Latencies in ms");
        printPValues(operationsTimeLatencies);
        System.out.println("---------------------------------------------------------------------------");
    }

    public  void  getServerLatencies(){
        System.out.println("latencies for sever operations in ms");
        printPValues(serverLatencies);
        System.out.println("---------------------------------------------------------------------------");
    }

    public  void  getCacheLatencies(){
        System.out.println("Latencies for cache operations in ms");
        printPValues(cacheLatencies);
        System.out.println("---------------------------------------------------------------------------");
    }

    // --------------------------------------------- Private Methods -------------------------------------------------

    //returns 0 (set) OR 1 (get) depending upon the requirements of reads and writes
    private boolean writeOperation() {
        int random = ThreadLocalRandom.current().nextInt(100);
        return random < writePercentage;
    }

    public Runnable createBenchmarkingRunnable(int index) {
        return () -> {
            try (CachedJedis jedis = getJedisInstance()) {
                List<Long> localOperationLatencies = new ArrayList<>();
                List<Long> serverSetLatencies = new ArrayList<>();
                List<Long> cacheGetLatencies = new ArrayList<>();

                long lowerBoundGroup = index * interval;
                long upperBoundGroup = index * (interval) + interval;
                String clientId = String.valueOf(jedis.clientId());
                //Populating Cache
                for (int i = 0; i < warmCacheIterations; i++) {
                    long randomKey = getRandomKey(upperBoundGroup, lowerBoundGroup);
                    jedis.get(KEY_PREFIX + randomKey);
                    checkStale.get(KEY_PREFIX + randomKey).add(clientId);
                }

                for (int i = 0; i < totalOperations; i++) {
                    long start;
                    long end;
                    long randomKey = getRandomKey(upperBoundGroup, lowerBoundGroup);
                    if (writeOperation()) {
                        //SET functionality
                        String randomString = BenchmarkingUtil.randomString(messageSize);
                        start = System.nanoTime();
                        jedis.set(KEY_PREFIX + randomKey, randomString);
                        end = System.nanoTime();
                        serverSetLatencies.add(end - start);
                        //Updating the value of clientId writing on the key
                        checkStale.get(KEY_PREFIX + randomKey).clear();
                    } else {
                        //GET functionality
                        totalGet.incrementAndGet();
                        start = System.nanoTime();
                        //Check if the key was accessed from the cache
                        Boolean flag = jedis.boolGet(KEY_PREFIX + randomKey);
                        end = System.nanoTime();
                        if (flag) {
                            boolean value = checkStale.get(KEY_PREFIX + randomKey).contains(clientId);
                            if (!value) {
                                staleCount.incrementAndGet();
                            }
                            //Check if the value is stale
                            cacheGetLatencies.add(end - start);
                            //key found in cache
                            cacheHit.incrementAndGet();
                        } else{
                            checkStale.get(KEY_PREFIX + randomKey).add(clientId);
                        }
                    }
                    localOperationLatencies.add(end - start);
                    try {
                        Thread.sleep(waitTime());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                serverLatencies.addAll(jedis.getServerGetLatencies());
                serverLatencies.addAll(serverSetLatencies);
                operationsTimeLatencies.addAll(localOperationLatencies);
                cacheLatencies.addAll(cacheGetLatencies);
                cacheLatencies.addAll(jedis.getPutInCacheLatencies());
            }
        };
    }

    private CachedJedis getJedisInstance() {
        CachedJedis cachedJedis = cachedJedisPool.getResource();
        populateCheckStale();
        return cachedJedis;
    }

    private void populateCheckStale(){
        for(int i = 0 ; i < totalKeys ; i++){
            checkStale.put(KEY_PREFIX + i , new HashSet<>());
        }
    }

    //returns true if the client will read from its group else false
    private boolean getFromGroup() {
        int random = ThreadLocalRandom.current().nextInt(100);
        return random < readFromGroup;
    }

    private long getRandomKey(long upperBound, long lowerBound) {
        if (getFromGroup()) {
            return (long) (ThreadLocalRandom.current().nextDouble() * (upperBound - lowerBound) + lowerBound);
        } else {
            return ThreadLocalRandom.current().nextLong(0, totalKeys);
        }
    }

    //returns a random wait time
    private long waitTime() {
        long value = (long) (ThreadLocalRandom.current().nextGaussian() * sigmaOperationTime + meanOperationTime);
        if(value < 0){
            return 0;
        }
        return value;
    }

    private void printPValues(List<Long> latencies) {
        Collections.sort(latencies);
        if(latencies.size() == 0) {
            return;
        }
        long p10Time = latencies.get((int) (latencies.size()*0.1));
        long p20Time = latencies.get((int) (latencies.size()*0.2));
        long p30Time = latencies.get((int) (latencies.size()*0.3));
        long p40Time = latencies.get((int) (latencies.size()*0.4));
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
        System.out.println("P10 "+p10Time/1000000.0);
        System.out.println("P25 "+p20Time/1000000.0);
        System.out.println("P30 "+p30Time/1000000.0);
        System.out.println("P40 "+p40Time/1000000.0);
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
