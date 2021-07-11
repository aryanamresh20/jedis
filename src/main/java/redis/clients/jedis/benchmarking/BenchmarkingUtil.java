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

    public static void populateKeys(String hostName, int portNumber, long totalKeys) {
        try (Jedis jedis = new Jedis(hostName, portNumber)) {
            for (int i = 0; i < totalKeys; i++) {
                //Populating the database with multiple number of keys
                jedis.setex(KEY_PREFIX + i, EXPIRY_SECONDS, "hello" + i);
            }
            jedis.quit();
        }
    }

    public static void populateHashes(String hostName, int portNumber, long totalKeys) {
        try (Jedis jedis = new Jedis(hostName, portNumber)) {
            for (int i = 0; i < totalKeys; i++) {
                //Populating the database with multiple number of keys
                Map<String , String> map = new HashMap<>();
                map.put("hello0", "world0"+i);
                map.put("hello1", "world1"+i);
                map.put("hello2", "world2"+i);
                map.put("hello3", "world3"+i);
                map.put("hello4", "world4"+i);
                map.put("hello5", "world5"+i);
                map.put("hello6", "world6"+i);
                map.put("hello7", "world7"+i);
                map.put("hello8", "world8"+i);
                map.put("hello9", "world9"+i);
                //Populating the database with multiple hash
                jedis.hset(HASH_KEY_PREFIX + i, map);
                jedis.expire(HASH_KEY_PREFIX + i, EXPIRY_SECONDS);
            }
            jedis.quit();
        }
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
