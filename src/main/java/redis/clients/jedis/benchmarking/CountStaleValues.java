package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class CountStaleValues {

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
    private final List<Long> operationsTime = new CopyOnWriteArrayList<>();
    //To keep track of the last client setting the key
    private final ConcurrentHashMap<String , String > checkStale = new ConcurrentHashMap<>();
    public CountStaleValues(String host , int port , long numberOfClients , long numberOfKeys , long readPercentage ,
                            long writePercentage , long numberOfOperations , long meanOperationTime , double sigmaOperationTime,
                            long expireAfterAccess , long expireAfterWrite , long messageLength , long initialCachePopulateIter) {
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
        for(int i = 0 ; i<messageSize ; i++){
            message.append(getRandom());
        }
        return String.valueOf(message);
    }

    //return random key Ids present in the database
    private int getRandom() {
        Random rand = new Random();
        return rand.nextInt((int) totalKeys);
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
        CachedJedis cachedJedis = new CachedJedis(hostName,portNumber);
        JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder().maxCacheSize(totalKeys*2)
                                            .expireAfterWrite(expireAfterWriteMillis)
                                            .expireAfterAccess(expireAfterAccessMillis)
                                            .build();
        cachedJedis.setupCaching(jedisCacheConfig);
        for(int i =0 ; i < initialCachePopulate ; i++){
            int randomKey = getRandom();
            //Cache populating
            cachedJedis.get(String.valueOf(randomKey));
        }
        String clientId = String.valueOf(cachedJedis.clientId());

        for(int i = 0 ; i < totalOperations ; i++) {
            long start = System.nanoTime();
            int randomGetSet = getBinaryRandom();
            if (randomGetSet == 0) {
                //SET functionality
                int randomKey = getRandom() ;
                cachedJedis.set(String.valueOf(randomKey) , "hello" + clientId);
                //Updating the value of clientId writing on the key

                checkStale.put(String.valueOf(randomKey) , clientId);
            } else {
                //GET functionality
                incTotalGet();
                int randomKey = getRandom();
                //Check if the key was accessed from the cache
                Boolean flag = cachedJedis.boolGet(String.valueOf(randomKey));
                if (flag) {
                    String value = checkStale.get(String.valueOf(randomKey));
                    //key found in cache
                    incCacheHit();
                    if(value != null) {
                        //Check if the value was stale
                        if ( !(value.equals(clientId)) ) {
                            incStaleCount();
                        }
                    }
                }
            }
            long end = System.nanoTime();
            operationsTime.add(end-start);
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
        cachedJedis.close();
    };
    public long getStaleCount(){ return staleCount;}
    public long getCacheHit() { return cacheHit;}
    public long getCacheMiss() { return totalGet-cacheHit;}
    public void getLatencies(){
        Collections.sort(operationsTime);
        long p50Time = operationsTime.get((int) (operationsTime.size()*0.5));
        long p75Time = operationsTime.get((int) (operationsTime.size()*0.75));
        long p90Time = operationsTime.get((int) (operationsTime.size()*0.90));
        long p95Time = operationsTime.get((int) (operationsTime.size()*0.95));
        long p97Time = operationsTime.get((int) (operationsTime.size()*0.97));
        long p99Time = operationsTime.get((int) (operationsTime.size()*0.99));
        System.out.println("Latencies in milliseconds");
        System.out.println("P50 "+p50Time/1000000.0);
        System.out.println("P75 "+p75Time/1000000.0);
        System.out.println("P90 "+p90Time/1000000.0);
        System.out.println("P95 "+p95Time/1000000.0);
        System.out.println("P97 "+p97Time/1000000.0);
        System.out.println("P99 "+p99Time/1000000.0);
    }

}
