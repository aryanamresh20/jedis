package redis.clients.jedis.benchmarking;

import redis.clients.jedis.JedisCacheConfig;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class ReadsMget {

    private final String hostName;
    private final int portNumber;
    private long begin;
    private long end;
    private final long totalKeys;
    public ReadsMget(String host,int port,long numberOfKeys) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
        populateDatabase();
    }

    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++) {
            //Populating the database with multiple keys
            jedis.set(String.valueOf(i) , "hello" + i);
        }
    }

    public long JedisTest() {
        Jedis jedisInstance = new Jedis(hostName,portNumber);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            List<String> mgetInstance = new ArrayList<String>();
            // Initial Reads ,these keys reads directly from the server
            mgetInstance.add(String.valueOf(i));
            for(int j = i ; j >= 0 ; j--){
                // No Caching available these keys also reads from the server creates delays
                mgetInstance.add(String.valueOf(j));
            }
            String[] mgetInstanceArray = new String[mgetInstance.size()];
            mgetInstanceArray = mgetInstance.toArray(mgetInstanceArray);
            jedisInstance.mget(mgetInstanceArray);
        }
        end = Calendar.getInstance().getTimeInMillis();
        jedisInstance.close();
        return (end-begin);
    }

    public long CacheJedisTest(){
        CachedJedis cachedJedisInstance = new CachedJedis(hostName,portNumber);
        JedisCacheConfig jedisCacheConfig = JedisCacheConfig.Builder.newBuilder()
                .maxCacheSize(totalKeys*2)
                .expireAfterAccess(1000)
                .expireAfterWrite(1000)
                .build();
        cachedJedisInstance.setupCaching(jedisCacheConfig);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            List<String> mgetInstance = new ArrayList<String>();
            // Initial Reads ,these keys reads directly from the server
            mgetInstance.add(String.valueOf(i));
            for(int j = i ; j >= 0 ; j--){
                //Caching available these keys reads from the local Cache
                mgetInstance.add(String.valueOf(j));
            }
            String[] mgetInstanceArray = new String[mgetInstance.size()];
            mgetInstanceArray = mgetInstance.toArray(mgetInstanceArray);
            cachedJedisInstance.mget(mgetInstanceArray);
        }
        end = Calendar.getInstance().getTimeInMillis();
        cachedJedisInstance.close();
        return (end-begin);
    }
}
