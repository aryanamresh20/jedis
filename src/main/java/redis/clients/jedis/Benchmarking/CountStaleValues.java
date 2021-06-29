package redis.clients.jedis.Benchmarking;

import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Random;

public class CountStaleValues {


    private Jedis jedis;
    private String hostName;
    private int portNumber;
    private int staleCount = 0;
    private int cacheHit = 0;
    private int cacheMiss = 0;
    private int totalGet = 0;
    private int reads ;
    private int writes;
    private int totalClients ;
    private int totalKeys;
    private int totalOperations;
    private HashMap<String , String> checkStale = new HashMap<String , String>(); //To keep track of the lat client setting the key
    public CountStaleValues(String host,int port,int numberOfClients , int numberOfKeys , int readPercentage , int writePercentage , int numberOfOperations) {
        totalClients = numberOfClients;
        reads = readPercentage;
        writes = writePercentage;
        totalKeys = numberOfKeys;
        hostName = host;
        portNumber = port;
        totalOperations = numberOfOperations;
        jedis = new Jedis(hostName,portNumber);
        populateDatabase();
        cacheJedisThreads();
    }
    private void populateDatabase(){
        for(int i=0; i<totalKeys ; i++){
            jedis.set(String.valueOf(i),"hello"+i); //Populate database with multiple keys
        }
    }
    //Starting multiple cacheJedis instances on multiple threads
    private void cacheJedisThreads(){
        for(int i=0; i<totalClients ;i ++){
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
    Runnable runnable = () -> {
        CacheJedis cacheJedis = new CacheJedis(hostName,portNumber);
        String clientId = String.valueOf(cacheJedis.clientId());
        for(int i=0;i<totalOperations;i++) {
            int randomGetSet = getBinaryRandom();

            if (randomGetSet == 0) { //SET functionality
                int randomKey = getRandom() ;
                cacheJedis.set(String.valueOf(randomKey), "hello" + clientId);
                checkStale.put(String.valueOf(randomKey), clientId); //Updating the value of clientId writing on the key
            } else { //GET functionality
                totalGet++;
                int randomKey = getRandom() ;
                Boolean flag = cacheJedis.boolGet(String.valueOf(randomKey)); //Check if the key was accessed from the cache
                if (flag) {
                    String value = checkStale.get(String.valueOf(randomKey));
                    cacheHit++;
                    if(value!=null) {
                        if (!(value.equals(clientId))) { //Check if the value was stale
                            staleCount++;
                        }
                    }
                }
            }
        }
        cacheJedis.close();
    };
    public int getStaleCount(){ return staleCount;}
    public int getCacheHit() { return cacheHit;}
    public int getCacheMiss() { return totalGet-cacheHit;}

}
