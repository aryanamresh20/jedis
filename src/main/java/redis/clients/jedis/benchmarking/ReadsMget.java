package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CacheConfig;
import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class ReadsMget {

    private String hostName;
    private int portNumber;
    private long begin;
    private long end;
    private int totalKeys;
    public ReadsMget(String host,int port,int numberOfKeys) {
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
        CacheJedis cacheJedisInstance = new CacheJedis(hostName,portNumber);
        CacheConfig cacheConfig = CacheConfig.Builder.newInstance().maxSize(totalKeys).build();
        cacheJedisInstance.enableCaching(cacheConfig);
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
            cacheJedisInstance.mget(mgetInstanceArray);
        }
        end = Calendar.getInstance().getTimeInMillis();
        cacheJedisInstance.close();
        return (end-begin);
    }
}
