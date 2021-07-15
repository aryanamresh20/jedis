package redis.clients.jedis.benchmarking;

import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

    public static Properties loadConfigFile(String[] args) throws Exception {
        InputStream inputStream = null;
        try {
            //config file for setting various properties
            if (args.length == 0) {
                String filePath = "default-benchmarking-config.properties";
                ClassLoader classLoader = BenchmarkingOnSingleClient.class.getClassLoader();
                inputStream = classLoader.getResourceAsStream(filePath);
            } else {
                inputStream = new FileInputStream(args[0]);
            }

            Properties props = new Properties();
            props.load(inputStream);
            return props;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

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
                IntStream.range(0, 10).forEach(l -> map.put("hello" + l, randomString(messageSize)));
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
                if(keys.getResult().size() != 0) {
                    jedis.unlink(keys.getResult().toArray(new String[0]));
                }
                cursor = keys.getCursor();
            } while (!cursor.equals(SCAN_POINTER_START));
            jedis.quit();
        }
    }

    public static void warmCache(CachedJedis cachedJedis, long warmCachePercentage, long totalKeys, boolean hashKeys) {

        long totalKeysToFilled = (long) ((warmCachePercentage/100.0)*totalKeys);
        ArrayList <Long> listOfkeys = new ArrayList<>();
        for(long i=0 ; i<totalKeys ; i++){
            listOfkeys.add(i);
        }
        Collections.shuffle(listOfkeys);
        for (int i = 0; i < totalKeysToFilled; i++) {
            long key = listOfkeys.get(i);
            // Initial Reads , reads directly from the server
            if (!hashKeys) {
                cachedJedis.get(KEY_PREFIX + key);
            } else {
                cachedJedis.hgetAll(HASH_KEY_PREFIX + key);
            }
        }
    }

    public static String randomString(long messageSize){
        StringBuilder message = new StringBuilder();
        for(int i = 0 ; i < messageSize ; i++){
            message.append(ThreadLocalRandom.current().nextInt(0, 10));
        }
        return String.valueOf(message);
    }
}
