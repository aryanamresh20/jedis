package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

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
    private final double sigmaWaitTime ;
    private final long meanWaitTime ;
    private long expireAfterAccessMillis;
    private long expireAfterWriteMillis;
    private final long messageSize;
    private long initialCachePopulate;
    private long interval;
    private final long readFromGroup;
    private final List<Long> operationsTimeLatencies = new CopyOnWriteArrayList<>();
    private final List<Long> staleTimeLatencies = new CopyOnWriteArrayList<>();
    private final List<Long> serverLatencies = new CopyOnWriteArrayList<>();
    private final List<Long> cacheLatencies = new CopyOnWriteArrayList<>();
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
        meanWaitTime = meanOperationTime;
        sigmaWaitTime = sigmaOperationTime;
        expireAfterAccessMillis = expireAfterAccess;
        expireAfterWriteMillis = expireAfterWrite;
        messageSize = messageLength;
        initialCachePopulate = initialCachePopulateIter;
        readFromGroup = readFromGroupPercentage;
        interval = totalKeys/totalClients;
        populateDatabase();
        startCacheJedisThreads();
    }
    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++){
            //Populate database with all the keys
            jedis.set(String.valueOf(i) , randomString());
        }
        jedis.close();
    }
    //Starting multiple cacheJedis instances on multiple threads
    private void startCacheJedisThreads()  {
        List<Thread> threadList = new ArrayList<>();
        for(int i = 0 ; i < totalClients ; i ++){
            //Waiting a random time to start each client
            try {
                TimeUnit.MILLISECONDS.sleep(waitTime());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
    //returns a random string of the length of message size
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

    //returns true if the client will read from its group else false
    private boolean getFromGroup() {
        Random rand = new Random();
        int random = rand.nextInt(100);
        if(random < readFromGroup){
            return true;
        } else{
            return false;
        }
    }

    //returns a random wait time
    private long waitTime(){
        Random rand = new Random();
        long value = (long) (rand.nextGaussian()*sigmaWaitTime+meanWaitTime);
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
        BenchmarkingCachedJedis benchmarkingCachedJedis = new BenchmarkingCachedJedis(hostName, portNumber);
        JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder().maxCacheSize(totalKeys*2) //cache will never be full
                                            .expireAfterWrite(expireAfterWriteMillis)
                                            .expireAfterAccess(expireAfterAccessMillis)
                                            .build();
        benchmarkingCachedJedis.setupCaching(jedisCacheConfig);

        List<Long> localOperationTimeLatencies = new ArrayList<>();
        List<Long> serverSetLatencies = new ArrayList<>();
        List<Long> cacheGetLatencies = new ArrayList<>();

        int index = Integer.parseInt(Thread.currentThread().getName().substring(14));
        long lowerBoundGroup = index*interval;
        long upperBoundGroup = index*(interval)+interval;
        String clientId = String.valueOf(benchmarkingCachedJedis.clientId());


        //Populating Cache
        for(int i =0 ; i < initialCachePopulate ; i++){
            int randomKey;
            if(getFromGroup()){
                randomKey = (int) (Math.random() * (upperBoundGroup - lowerBoundGroup) + lowerBoundGroup);
            }else{
                randomKey = getRandom();
            }
            benchmarkingCachedJedis.get(String.valueOf(randomKey));
        }


        for(int i = 0 ; i < totalOperations ; i++) {
            long start; //start time
            long end; //end time
            int randomGetSet = getBinaryRandom(); // GET or SET
            int randomKey;
            if(getFromGroup()){
                randomKey = (int) (Math.random() * (upperBoundGroup - lowerBoundGroup) + lowerBoundGroup);
            }else{
                randomKey = getRandom();
            }
            if (randomGetSet == 0) {
                //SET functionality
                String randomString = randomString();
                start = System.nanoTime();
                benchmarkingCachedJedis.set(String.valueOf(randomKey) , randomString);
                end = System.nanoTime();
                serverSetLatencies.add(end-start);
                //Updating the value of clientId writing on the key
                checkStale.put(String.valueOf(randomKey) , clientId);
            } else {
                //GET functionality
                incTotalGet();
                start = System.nanoTime();
                //Check if the key was accessed from the cache
                Boolean flag = benchmarkingCachedJedis.boolGet(String.valueOf(randomKey));
                end = System.nanoTime();
                if (flag) {
                    String value = checkStale.get(String.valueOf(randomKey));
                    cacheGetLatencies.add(end-start);
                    //key found in cache
                    incCacheHit();
                    if(value != null) {
                        //Check if the value was stale
                        if ( !(value.equals(clientId)) ) {
                            incStaleCount();
                            //Set the time when the key was stale
                           if(benchmarkingCachedJedis.staleTime.get(String.valueOf(randomKey))==null){
                                benchmarkingCachedJedis.staleTime.put(String.valueOf(randomKey),System.nanoTime());
                           }
                        }
                    }
                }
            }
            localOperationTimeLatencies.add(end-start);
            long waitTime = waitTime();
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        serverLatencies.addAll(benchmarkingCachedJedis.serverGetLatencies);
        serverLatencies.addAll(serverSetLatencies);
        operationsTimeLatencies.addAll(localOperationTimeLatencies);
        staleTimeLatencies.addAll(benchmarkingCachedJedis.staleTimeLatencies);
        cacheLatencies.addAll(cacheGetLatencies);
        cacheLatencies.addAll(benchmarkingCachedJedis.putInCacheLatencies);
        benchmarkingCachedJedis.close();
    };
    public long getStaleCount(){ return staleCount;}
    public long getCacheHit() { return cacheHit;}
    public long getCacheMiss() { return totalGet-cacheHit;}

    public void getOverallLatencies(){
        Collections.sort(operationsTimeLatencies);
        long p10Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.1));
        long p20Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.2));
        long p30Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.3));
        long p40Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.4));
        long p50Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.5));
        long p75Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.75));
        long p90Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.90));
        long p95Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.95));
        long p97Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.97));
        long p99Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.99));
        long p995Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.995));
        long p997Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.997));
        long p999Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.999));
        System.out.println("Overall Latencies in ms");
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
        System.out.println("P100 "+operationsTimeLatencies.get(operationsTimeLatencies.size()-1)/1000000.0);
        double average = (operationsTimeLatencies.stream().mapToDouble(d->d).average()).orElse(-1);
        System.out.println("Average Overall latency "+average/1000000.0);
        System.out.println("---------------------------------------------------------------------------");
    }

    public  void  getStaleLatencies(){
        Collections.sort(staleTimeLatencies);
        if(staleTimeLatencies.size()==0){ return; }
        long p50Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.5));
        long p75Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.75));
        long p90Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.90));
        long p95Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.95));
        long p97Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.97));
        long p99Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.99));
        long p995Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.995));
        long p997Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.997));
        long p999Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.999));
        long p9995Time = staleTimeLatencies.get((int) (staleTimeLatencies.size()*0.9995));
        long p100Time = staleTimeLatencies.get((staleTimeLatencies.size()-1));
        System.out.println("Stale Latencies in seconds");
        System.out.println("P50 "+p50Time/1000000000.0);
        System.out.println("P75 "+p75Time/1000000000.0);
        System.out.println("P90 "+p90Time/1000000000.0);
        System.out.println("P95 "+p95Time/1000000000.0);
        System.out.println("P97 "+p97Time/1000000000.0);
        System.out.println("P99 "+p99Time/1000000000.0);
        System.out.println("P99.5 "+p995Time/1000000000.0);
        System.out.println("P99.7 "+p997Time/1000000000.0);
        System.out.println("P99.9 "+p999Time/1000000000.0);
        System.out.println("P99.95 "+p9995Time/1000000000.0);
        System.out.println("P100 "+p100Time/1000000000.0);
        double average = (staleTimeLatencies.stream().mapToDouble(d->d).average()).orElse(-1);
        System.out.println("Average Stale time latency "+average/1000000000.0);
        System.out.println("---------------------------------------------------------------------------");
    }

    public  void  getServerLatencies(){
        Collections.sort(serverLatencies);
        if(serverLatencies.size()==0){ return; }
        long p50Time = serverLatencies.get((int) (serverLatencies.size()*0.5));
        long p75Time = serverLatencies.get((int) (serverLatencies.size()*0.75));
        long p90Time = serverLatencies.get((int) (serverLatencies.size()*0.90));
        long p95Time = serverLatencies.get((int) (serverLatencies.size()*0.95));
        long p97Time = serverLatencies.get((int) (serverLatencies.size()*0.97));
        long p99Time = serverLatencies.get((int) (serverLatencies.size()*0.99));
        long p995Time = serverLatencies.get((int) (serverLatencies.size()*0.995));
        long p997Time = serverLatencies.get((int) (serverLatencies.size()*0.997));
        long p999Time = serverLatencies.get((int) (serverLatencies.size()*0.999));
        long p9995Time = serverLatencies.get((int) (serverLatencies.size()*0.9995));
        long p100Time = serverLatencies.get((serverLatencies.size()-1));
        System.out.println("latencies for sever operations in ms");
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
        double average = (serverLatencies.stream().mapToDouble(d->d).average()).orElse(-1);
        System.out.println("Average Server Hit latency "+average/1000000.0);
        System.out.println("---------------------------------------------------------------------------");
    }

    public  void  getCacheLatencies(){
        Collections.sort(cacheLatencies);
        if(cacheLatencies.size()==0){ return; }
        long p50Time = cacheLatencies.get((int) (cacheLatencies.size()*0.5));
        long p75Time = cacheLatencies.get((int) (cacheLatencies.size()*0.75));
        long p90Time = cacheLatencies.get((int) (cacheLatencies.size()*0.90));
        long p95Time = cacheLatencies.get((int) (cacheLatencies.size()*0.95));
        long p97Time = cacheLatencies.get((int) (cacheLatencies.size()*0.97));
        long p99Time = cacheLatencies.get((int) (cacheLatencies.size()*0.99));
        long p995Time = cacheLatencies.get((int) (cacheLatencies.size()*0.995));
        long p997Time = cacheLatencies.get((int) (cacheLatencies.size()*0.997));
        long p999Time = cacheLatencies.get((int) (cacheLatencies.size()*0.999));
        long p9995Time = cacheLatencies.get((int) (cacheLatencies.size()*0.9995));
        long p100Time = cacheLatencies.get((cacheLatencies.size()-1));
        System.out.println("Latencies for cache operations in ms");
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
        double average = (cacheLatencies.stream().mapToDouble(d->d).average()).orElse(-1);
        System.out.println("Average Cache Hit latency "+average/1000000.0);
        System.out.println("---------------------------------------------------------------------------");
    }
}
