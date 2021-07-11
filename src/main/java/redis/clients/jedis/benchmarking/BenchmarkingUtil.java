package redis.clients.jedis.benchmarking;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.HASH_KEY_PREFIX;
import static redis.clients.jedis.benchmarking.BenchmarkingCachedJedis.KEY_PREFIX;

/**
 * User: manas.joshi
 * Date: 11-07-2021
 * Time: 1:00 PM
 */
public class BenchmarkingUtil {

    private static final long EXPIRY_SECONDS = TimeUnit.HOURS.toSeconds(5);

    public static void populateKeys(String hostName, int portNumber, long totalKeys, long messageSize) {
        try (Jedis jedis = new Jedis(hostName, portNumber)) {
            for (int i = 0; i < totalKeys; i++) {
                //Populating the database with multiple number of keys
                jedis.setex(KEY_PREFIX + i, EXPIRY_SECONDS, randomString(messageSize));
            }
            jedis.quit();
        }
    }

    public static void populateHashes(String hostName, int portNumber, long totalKeys, long messageSize) {
        try (Jedis jedis = new Jedis(hostName, portNumber)) {
            for (int i = 0; i < totalKeys; i++) {
                //Populating the database with multiple number of keys
                Map<String , String> map = new HashMap<>();
                map.put("hello0", randomString(messageSize));
                map.put("hello1", randomString(messageSize));
                map.put("hello2", randomString(messageSize));
                map.put("hello3", randomString(messageSize));
                map.put("hello4", randomString(messageSize));
                map.put("hello5", randomString(messageSize));
                map.put("hello6", randomString(messageSize));
                map.put("hello7", randomString(messageSize));
                map.put("hello8", randomString(messageSize));
                map.put("hello9", randomString(messageSize));
                //Populating the database with multiple hash
                jedis.hset(HASH_KEY_PREFIX + i, map);
                jedis.expire(HASH_KEY_PREFIX + i, EXPIRY_SECONDS);
            }
            jedis.quit();
        }
    }

    private static String randomString(long messageSize){
        StringBuilder message = new StringBuilder();
        for(int i = 0 ; i < messageSize ; i++){
            message.append(Math.random());
        }
        return String.valueOf(message);
    }

    public static void cleanDatabase(String hostName, int portNumber) {
        try (Jedis jedis = new Jedis(hostName, portNumber)) {
            ScanParams params = new ScanParams().match(KEY_PREFIX + "*").count(1000);
            String cursor = SCAN_POINTER_START;
            do {
                ScanResult<String> keys = jedis.scan(cursor, params);
                jedis.unlink(keys.getResult().toArray(new String[0]));
                cursor = keys.getCursor();
            } while (!cursor.equals(SCAN_POINTER_START));
            jedis.quit();
        }
    }
}
