package redis.clients.jedis.Benchmarking;

import redis.clients.jedis.CacheJedis;
import redis.clients.jedis.Jedis;

import java.util.Calendar;

public class SingleReads {

    private Jedis jedis ;
    private String hostName;
    private int portNumber;
    private long begin;
    private long end;
    private int totalKeys;
    public SingleReads(String host,int port,int numberOfKeys) {
        hostName = host;
        portNumber = port;
        jedis = new Jedis(hostName,portNumber);
        totalKeys = numberOfKeys;
        for(int i=0 ; i<numberOfKeys ; i++){
            jedis.set(String.valueOf(i),"hello"+i);
        }
    }

    public long JedisTest() {
        Jedis jedisInstance = new Jedis(hostName,portNumber);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i=0 ; i<totalKeys ; i++){
            jedisInstance.get(String.valueOf(i));
        }
        end = Calendar.getInstance().getTimeInMillis();
        jedisInstance.close();
        return (end-begin);
    }

    public long CacheJedisTest(){
        CacheJedis cacheJedisInstance = new CacheJedis(hostName,portNumber);
        begin = Calendar.getInstance().getTimeInMillis();
        for(int i=0 ; i<totalKeys ; i++){
            cacheJedisInstance.get(String.valueOf(i));
        }
        end = Calendar.getInstance().getTimeInMillis();
        cacheJedisInstance.close();
        return (end-begin);
    }
}
