package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class CountStaleValues {

    private String hostName;
    private int portNumber;
    private volatile int staleCount = 0;
    private volatile int cacheHit = 0;
    private volatile int totalGet = 0;
    private int reads ;
    private int writes;
    private int totalClients ;
    private int totalKeys;
    private int totalOperations;
    private double sigma ;
    private int mean ;
    //To keep track of the last client setting the key
    private ConcurrentHashMap<String , String> checkStale = new ConcurrentHashMap<>();
    public CountStaleValues(String host,int port,int numberOfClients , int numberOfKeys , int readPercentage , int writePercentage , int numberOfOperations , int meanOperationTime , double sigmaOperationTime) {
        totalClients = numberOfClients;
        reads = readPercentage;
        writes = writePercentage;
        totalKeys = numberOfKeys;
        hostName = host;
        portNumber = port;
        totalOperations = numberOfOperations;
        mean = meanOperationTime;
        sigma = sigmaOperationTime;
        populateDatabase();
        cacheJedisThreads();
    }
    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++){
            //Populate database with multiple keys
            jedis.set(String.valueOf(i) , "hello"+i);
        }
        jedis.close();
    }
    //Starting multiple cacheJedis instances on multiple threads
    private void cacheJedisThreads()  {
        for(int i = 0 ; i < totalClients ; i ++){
            Thread thread =new Thread(runnable);
            thread.setName("thread"+i);
            thread.start();

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
    //return random key Ids present in the database
    private int getRandom() {
        Random rand = new Random();
        return rand.nextInt(totalKeys);
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
        JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder().maxCacheSize(totalKeys).build();
        cachedJedis.setupCaching(jedisCacheConfig);
        String clientId = String.valueOf(cachedJedis.clientId());
        Thread thread = Thread.currentThread();

        for(int i = 0 ; i < totalOperations ; i++) {
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
            long waitTime = waitTime();
            if(waitTime < 0){
                waitTime = 0;
            }
            try {
                thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cachedJedis.close();
    };
    public int getStaleCount(){ return staleCount;}
    public int getCacheHit() { return cacheHit;}
    public int getCacheMiss() { return totalGet-cacheHit;}

}
