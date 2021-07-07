package redis.clients.jedis.benchmarking;

import javafx.util.Pair;
import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class CachedJedisLatencies {

    private String hostName;
    private int portNumber;
    private volatile long staleCount = 0;
    private volatile long cacheHit = 0;
    private volatile int totalGet = 0;
    private final long reads ;
    private final long writes;
    private final long totalClients ;
    private long totalKeys;
    private long totalOperations;
    private final double sigma ;
    private final long mean ;
    private long expireAfterAccessMillis;
    private long expireAfterWriteMillis;
    private final long messageSize;
    private long initialCachePopulate;
    private long interval;
    private final long readFromGroup;
    private final List<Long> operationsTime = new CopyOnWriteArrayList<>();
    private final List<Long> staleTime = new CopyOnWriteArrayList<>();
    //To keep track of the last client setting the key
    private final ConcurrentHashMap<String , String > checkStale = new ConcurrentHashMap<>();
    public CachedJedisLatencies(String host , int port , long numberOfClients , long numberOfKeys , long readPercentage ,
                                long writePercentage , long numberOfOperations , long meanOperationTime , double sigmaOperationTime,
                                long expireAfterAccess , long expireAfterWrite , long messageLength , long initialCachePopulateIter ,
                                long readFromGroupPercentage) {
        totalClients = numberOfClients;
        reads = readPercentage;
        writes = writePercentage;
        totalKeys = numberOfKeys;
        hostName = host;
        portNumber = port;
        totalOperations = numberOfOperations;
        mean = meanOperationTime;
        sigma = sigmaOperationTime;
        expireAfterAccessMillis = expireAfterAccess;
        expireAfterWriteMillis = expireAfterWrite;
        messageSize = messageLength;
        initialCachePopulate = initialCachePopulateIter;
        readFromGroup = readFromGroupPercentage;
        interval = totalKeys/totalClients;
        populateDatabase();
        cacheJedisThreads();
    }
    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++){
            //Populate database with multiple keys
            jedis.set(String.valueOf(i) , randomString());
        }
        jedis.close();
    }
    //Starting multiple cacheJedis instances on multiple threads
    private void cacheJedisThreads() {
        List<Thread> threadList = new ArrayList<>();
        for(int i = 0 ; i < totalClients ; i ++){
            Thread thread =new Thread(runnable);
            threadList.add(thread);
            thread.setName("CLIENT_THREAD "+i);
            thread.start();
        }
        for(int i = 0 ; i < totalClients ; i ++){
            try {
                threadList.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    //returns 0 (set) OR 1 (get) depending upon the requirements of reads and writes
    private int getBinaryRandom() {
        Random rand = new Random();
        int random = rand.nextInt(100);
        if(random < writes){
            return 0;
        }else{
            return 1;
        }
    }
    private String randomString(){
        StringBuilder message = new StringBuilder();
        for(int i = 0 ; i < messageSize ; i++){
            message.append(getRandom());
        }
        return String.valueOf(message);
    }

    //return random key Ids present in the database
    private int getRandom() {
        Random rand = new Random();
        return rand.nextInt((int) totalKeys);
    }
    private boolean getFromGroup() {
        Random rand = new Random();
        int random = rand.nextInt(100);
        if(random<readFromGroup){
            return true;
        } else{
            return false;
        }
    }
    private long waitTime(){
        Random rand = new Random();
        long value = (long) (rand.nextGaussian()*sigma+mean);
        if(value < 0){
            return 0;
        }
        return value;
    }

    private synchronized void incStaleCount(){ staleCount++; }
    private synchronized void incTotalGet(){ totalGet++; }
    private synchronized void incCacheHit() { cacheHit++; }

    //Runnable for each thread
    Runnable runnable = () -> {
       BenchmarkingCachedJedis cachedJedis = new BenchmarkingCachedJedis(hostName, portNumber);
        JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder().maxCacheSize(totalKeys*2)
                                            .expireAfterWrite(expireAfterWriteMillis)
                                            .expireAfterAccess(expireAfterAccessMillis)
                                            .build();
        cachedJedis.setupCaching(jedisCacheConfig);
        List<Long> localOperationTime = new ArrayList<>();
        Random rand = new Random();
        int index = Integer.parseInt(Thread.currentThread().getName().substring(14));
        long lowerBound = index*interval;
        long upperBound = index*(interval)+interval;
        long start;
        long end;
        String clientId = String.valueOf(cachedJedis.clientId());

        for(int i =0 ; i < initialCachePopulate ; i++){
            int randomKey;
            if(getFromGroup()){
                randomKey = (int) (Math.random() * (upperBound - lowerBound) + lowerBound);
            }else{
                randomKey = rand.nextInt((int) totalKeys);
            }
            cachedJedis.set(String.valueOf(randomKey) , randomString());
        }


        for(int i = 0 ; i < totalOperations ; i++) {

            int randomGetSet = getBinaryRandom();
            int randomKey;
            if (randomGetSet == 0) {
                if(getFromGroup()){
                   randomKey = (int) (Math.random() * (upperBound - lowerBound) + lowerBound);
                }else{
                   randomKey = rand.nextInt((int) totalKeys);
                }
                String randomString = randomString();
                start = System.nanoTime();
                cachedJedis.set(String.valueOf(randomKey) , randomString);
                end = System.nanoTime();
                //Updating the value of clientId writing on the key
                checkStale.put(String.valueOf(randomKey) , clientId);
            } else {
                //GET functionality
                incTotalGet();
                if(getFromGroup()){
                    randomKey = (int) (Math.random() * (upperBound - lowerBound) + lowerBound);
                }else{
                    randomKey = rand.nextInt((int) totalKeys);
                }
                start = System.nanoTime();
                //Check if the key was accessed from the cache
                Boolean flag = cachedJedis.boolGet(String.valueOf(randomKey));
                end = System.nanoTime();
                if (flag) {
                    String value = checkStale.get(String.valueOf(randomKey));
                    //key found in cache
                    incCacheHit();
                    if(value != null) {
                        //Check if the value was stale
                        if ( !(value.equals(clientId)) ) {
                            incStaleCount();
                           if(cachedJedis.staleTime.get(String.valueOf(randomKey))==null){
                                cachedJedis.staleTime.put(String.valueOf(randomKey),System.currentTimeMillis());
                           }
                        }
                    }
                }
            }
            localOperationTime.add(end-start);
            long waitTime = waitTime();
            if(waitTime < 0){
                waitTime = 0;
            }
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        operationsTime.addAll(localOperationTime);
        staleTime.addAll(cachedJedis.staleTimes);
        cachedJedis.close();
    };
    public long getStaleCount(){ return staleCount;}
    public long getCacheHit() { return cacheHit;}
    public long getCacheMiss() { return totalGet-cacheHit;}
    public void getLatencies(){
        Collections.sort(operationsTime);
        long p10Time = operationsTime.get((int) (operationsTime.size()*0.1));
        long p20Time = operationsTime.get((int) (operationsTime.size()*0.2));
        long p30Time = operationsTime.get((int) (operationsTime.size()*0.3));
        long p40Time = operationsTime.get((int) (operationsTime.size()*0.4));
        long p50Time = operationsTime.get((int) (operationsTime.size()*0.5));
        long p75Time = operationsTime.get((int) (operationsTime.size()*0.75));
        long p90Time = operationsTime.get((int) (operationsTime.size()*0.90));
        long p95Time = operationsTime.get((int) (operationsTime.size()*0.95));
        long p97Time = operationsTime.get((int) (operationsTime.size()*0.97));
        long p99Time = operationsTime.get((int) (operationsTime.size()*0.99));
        long p995Time = operationsTime.get((int) (operationsTime.size()*0.995));
        long p997Time = operationsTime.get((int) (operationsTime.size()*0.997));
        long p999Time = operationsTime.get((int) (operationsTime.size()*0.999));
        System.out.println("Latencies in milliseconds");
        System.out.println("P10 "+p10Time/1000000.0);
        System.out.println("P25 "+p20Time/1000000.0);
        System.out.println("P30 "+p30Time/1000000.0);
        System.out.println("P45 "+p40Time/1000000.0);
        System.out.println("P50 "+p50Time/1000000.0);
        System.out.println("P75 "+p75Time/1000000.0);
        System.out.println("P90 "+p90Time/1000000.0);
        System.out.println("P95 "+p95Time/1000000.0);
        System.out.println("P97 "+p97Time/1000000.0);
        System.out.println("P99 "+p99Time/1000000.0);
        System.out.println("P99.5 "+p995Time/1000000.0);
        System.out.println("P99.7 "+p997Time/1000000.0);
        System.out.println("P99.9 "+p999Time/1000000.0);
        System.out.println("Average latency "+operationsTime.stream().mapToDouble(d->d).average());
    }

    public  void  getStaleLatencies(){
        Collections.sort(staleTime);
        if(staleTime.size()==0){ return; }
        long p50Time = staleTime.get((int) (staleTime.size()*0.5));
        long p75Time = staleTime.get((int) (staleTime.size()*0.75));
        long p90Time = staleTime.get((int) (staleTime.size()*0.90));
        long p95Time = staleTime.get((int) (staleTime.size()*0.95));
        long p97Time = staleTime.get((int) (staleTime.size()*0.97));
        long p99Time = staleTime.get((int) (staleTime.size()*0.99));
        long p995Time = staleTime.get((int) (staleTime.size()*0.995));
        long p997Time = staleTime.get((int) (staleTime.size()*0.997));
        long p999Time = staleTime.get((int) (staleTime.size()*0.999));
        long p9995Time = staleTime.get((int) (staleTime.size()*0.9995));
        long p100Time = staleTime.get((staleTime.size()-1));
        System.out.println("Stale Latencies in milliseconds");
        System.out.println("P50 "+p50Time/1000000.0);
        System.out.println("P75 "+p75Time/1000000.0);
        System.out.println("P90 "+p90Time/1000000.0);
        System.out.println("P95 "+p95Time/1000000.0);
        System.out.println("P97 "+p97Time/1000000.0);
        System.out.println("P99 "+p99Time/1000000.0);
        System.out.println("P99.5 "+p995Time/1000000.0);
        System.out.println("P99.7 "+p997Time/1000000.0);
        System.out.println("P99.9 "+p999Time/1000000.0);
        System.out.println("P99.95 "+p9995Time/1000000.0);
        System.out.println("P100 "+p100Time/1000000.0);
    }

}
