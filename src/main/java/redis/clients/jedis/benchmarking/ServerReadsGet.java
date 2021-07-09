package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCacheConfig;

import java.util.Calendar;

public class ServerReadsGet {

    private final String hostName;
    private final int portNumber;
    private long begin;
    private long end;
    private final long totalKeys;
    public ServerReadsGet(String host, int port, long numberOfKeys) {
        hostName = host;
        portNumber = port;
        totalKeys = numberOfKeys;
        populateDatabase();
    }
    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++){
            //Populating the database with multiple number of keys
            jedis.set(String.valueOf(i) , "hello"+i);
        }
    }
    public long JedisTest() {
        Jedis jedisInstance = new Jedis(hostName,portNumber);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            //Single reads , reads directly from the server
            jedisInstance.get(String.valueOf(i));
        }
        end = Calendar.getInstance().getTimeInMillis();
        jedisInstance.close();
        return (end-begin);
    }

    public long CacheJedisTest(){
        CachedJedis cachedJedisInstance = new CachedJedis(hostName,portNumber);
        cachedJedisInstance.setupCaching(JedisCacheConfig.Builder.newBuilder().build());
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i = 0 ; i < totalKeys ; i++){
            //Single reads , reads directly from the server , keys not cached
            cachedJedisInstance.get(String.valueOf(i));
        }
        end = Calendar.getInstance().getTimeInMillis();
        cachedJedisInstance.close();
        return (end-begin);
    }
}
