package redis.clients.jedis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static redis.clients.jedis.JedisCacheConfig.TrackingMode.BROADCASTING;
import static redis.clients.jedis.Protocol.Command.CLIENT;

/**
 * Implements two connections mode of server assisted client side caching
 * https://redis.io/topics/client-side-caching
 *
 * This implementation is not thred safe
 */
public class CachedJedis extends Jedis {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachedJedis.class);

    private static final String INVALIDATION_CHANNEL = "__redis__:invalidate";
    private static final String POISON_PILL = "__POISON_PILL__";
    private static final Object DUMMY = new Object();

    private final Jedis invalidationConnection;
    private Cache<String , Object> cache;
    private volatile boolean cachingEnabled;
    private Long clientId;

    public CachedJedis() {
        super();
        invalidationConnection = new Jedis();
    }

    public CachedJedis(String uri) {
        super(uri);
        invalidationConnection = new Jedis(uri);
    }

    public CachedJedis(HostAndPort hp) {
        super(hp);
        invalidationConnection = new Jedis(hp);
    }

    public CachedJedis(HostAndPort hp, JedisClientConfig config) {
        super(hp, config);
        invalidationConnection = new Jedis(hp, config);
    }

    public CachedJedis(String host, int port) {
        super(host, port);
        invalidationConnection = new Jedis(host,port);
    }

    public CachedJedis(String host, int port, boolean ssl) {
        super(host, port, ssl);
        invalidationConnection = new Jedis(host, port, ssl);
    }

    public CachedJedis(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory,
                       SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(String host, int port, int timeout) {
        super(host, port, timeout);
        invalidationConnection = new Jedis(host, port, timeout);
    }

    public CachedJedis(String host, int port, int timeout, boolean ssl) {
        super(host, port, timeout, ssl);
        invalidationConnection = new Jedis(host, port, timeout, ssl);
    }

    public CachedJedis(String host, int port, int timeout, boolean ssl, SSLSocketFactory sslSocketFactory,
                       SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(String host, int port, int connectionTimeout, int soTimeout) {
        super(host, port, connectionTimeout, soTimeout);
        invalidationConnection = new Jedis(host, port, connectionTimeout, soTimeout);
    }

    public CachedJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
        invalidationConnection = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
    }

    public CachedJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl) {
        super(host, port, connectionTimeout, soTimeout, ssl);
        invalidationConnection = new Jedis(host, port, connectionTimeout, soTimeout, ssl);
    }

    public CachedJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl,
                       SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(host, port, connectionTimeout, soTimeout, ssl,
                                           sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout, boolean ssl,
                       SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout,
                                           ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(JedisShardInfo shardInfo) {
        super(shardInfo);
        invalidationConnection = new Jedis(shardInfo);
    }

    public CachedJedis(URI uri) {
        super(uri);
        invalidationConnection = new Jedis(uri);
    }

    public CachedJedis(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(URI uri, int timeout) {
        super(uri, timeout);
        invalidationConnection = new Jedis(uri, timeout);
    }

    public CachedJedis(URI uri, int timeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                       HostnameVerifier hostnameVerifier) {
        super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(URI uri, int connectionTimeout, int soTimeout) {
        super(uri, connectionTimeout, soTimeout);
        invalidationConnection = new Jedis(uri, connectionTimeout, soTimeout);
    }

    public CachedJedis(URI uri, int connectionTimeout, int soTimeout, SSLSocketFactory sslSocketFactory,
                       SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(URI uri, int connectionTimeout, int soTimeout, int infiniteSoTimeout
        , SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        invalidationConnection = new Jedis(uri, connectionTimeout, soTimeout, infiniteSoTimeout,
                                           sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedis(URI uri, JedisClientConfig config) {
        super(uri, config);
        invalidationConnection = new Jedis(uri, config);
    }

    public CachedJedis(JedisSocketFactory jedisSocketFactory) {
        super(jedisSocketFactory);
        invalidationConnection = new Jedis(jedisSocketFactory);
    }

    public CachedJedis(JedisSocketFactory jedisSocketFactory, JedisClientConfig clientConfig) {
        super(jedisSocketFactory, clientConfig);
        invalidationConnection = new Jedis(jedisSocketFactory, clientConfig);
    }

    public void setupCaching(JedisCacheConfig jedisCacheConfig) {
        if (jedisCacheConfig.isNoLoop() ||
            jedisCacheConfig.isOptInCaching() ||
            jedisCacheConfig.getTrackingMode() == BROADCASTING) {
            throw new UnsupportedOperationException("Config options not yet supported");
        }
        cachingEnabled = true;
        initClientTracking(jedisCacheConfig);
        cache = CacheBuilder.newBuilder()
                .maximumSize(jedisCacheConfig.getMaxCacheSize()).concurrencyLevel(64)
                .expireAfterAccess(jedisCacheConfig.getExpireAfterAccessMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(jedisCacheConfig.getExpireAfterWriteMillis(), TimeUnit.MILLISECONDS)
                .build();
        setupInvalidationPubSub();
    }

    @Override
    public String get(String key) {
        if(cachingEnabled) {
            Object cachedValue = getFromCache(key);
            if (cachedValue != null) {
                checkIsInMultiOrPipeline();
                //Found in cache
                if (cachedValue == DUMMY) {
                    return null;
                }
                return String.valueOf(cachedValue);
            } else {
                //Getting from server
                String valueFromServer = super.get(key);
                if (valueFromServer == null) {
                    putInCache(key, DUMMY);
                } else {
                    putInCache(key, valueFromServer);
                }
                return valueFromServer;
            }
        } else {
            return super.get(key);
        }
    }

    @Override
    public List<String> mget(String... keys) {
        checkIsInMultiOrPipeline();
        if(cachingEnabled) {
            List<String> finalValue = new ArrayList<>();
            List<String> keysNotInCache = new ArrayList<>();
            for (String key : keys) {
                Object cachedValue = getFromCache(key);
                if (cachedValue != null) {
                    //Directly get the result if available from the cache
                    finalValue.add(String.valueOf(cachedValue));
                } else {
                    //Initializing the key values that would be returned from the server as null
                    finalValue.add(null);
                    //If not in cache add in the parameter send to server
                    keysNotInCache.add(key);
                }
            }
            //Converting into compatible parameter for mget command
            String[] keysNotInCacheArray = keysNotInCache.toArray(new String[0]);
            List<String> valuesFromServer;
            if (keysNotInCacheArray.length != 0) {
                //Calling from the server
                valuesFromServer = super.mget(keysNotInCacheArray);
                int indexValuesFromServer = 0;
                for (int index = 0; index < keys.length; index++) {
                    if (finalValue.get(index) == null) {
                        //Adding the values returned from server in the null places to maintain order
                        String valueAtindex = valuesFromServer.get(indexValuesFromServer);
                        finalValue.set(index, valueAtindex);
                        if (valueAtindex == null) {
                            putInCache(keys[index], DUMMY);
                        } else {
                            putInCache(keys[index], valuesFromServer.get(indexValuesFromServer));
                        }
                        indexValuesFromServer++;
                    }
                }
            }
            for (int index = 0; index < finalValue.size(); index++) {
                if (finalValue.get(index) != null && finalValue.get(index).equals(String.valueOf(DUMMY))) {
                    finalValue.set(index, null);
                }
            }
            return finalValue;
        } else {
            return super.mget(keys);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        checkIsInMultiOrPipeline();
        if(cachingEnabled) {
            Object cachedValue = getFromCache(key);
            if (cachedValue != null) {
                if (cachedValue == DUMMY) {
                    return new HashMap<>();
                }
                // noinspection unchecked
                return (Map<String, String>) cachedValue;
            } else {
                Map<String, String> valueFromServer = super.hgetAll(key);
                if (valueFromServer.size() == 0) {
                    putInCache(key, DUMMY);
                } else {
                    putInCache(key, valueFromServer);
                }
                return valueFromServer;
            }
        } else {
            return super.hgetAll(key);
        }
    }

    @Override
    public String quit() {
        if(cachingEnabled) {
            pushPoisonPill();
        }
        return super.quit();
    }

    @Override
    public void close() {
        if(cachingEnabled) {
            pushPoisonPill();
        }
        super.close();
    }



    @VisibleForTesting
    protected void invalidateCache(String key) {
        cache.invalidate(key);
    }

    @VisibleForTesting
    public long getCacheSize() {
        if (cachingEnabled) {
            return cache.size();
        } else {
            return 0;
        }
    }
    @VisibleForTesting
    public boolean getIfCachingEnabled(){ return cachingEnabled; }

    // --------------------------------------------- Protected Methods -------------------------------------------------

    protected void putInCache(String key, Object value) {
        if(cachingEnabled) {
            cache.asMap().put(key,value);
        }
    }

    protected Object getFromCache(String key){
        if(!cachingEnabled) {
            return null;
        } else {
            return cache.getIfPresent(key);
        }
    }

    // --------------------------------------------- Private Methods -------------------------------------------------

    private void initClientTracking(JedisCacheConfig jedisCacheConfig) {
        if (invalidationConnection == null) {
            throw new IllegalArgumentException("Invalidation connection is not yet initialized");
        }

        clientId = invalidationConnection.clientId();
        switch (jedisCacheConfig.getTrackingMode()) {
            case DEFAULT:
                //CLIENT TRACKING REDIRECT ON clientId
                byte[][] clientTrackingArgs = new byte[][]{
                    SafeEncoder.encode("TRACKING"),
                    SafeEncoder.encode("ON"),
                    SafeEncoder.encode("REDIRECT"),
                    SafeEncoder.encode(String.valueOf(invalidationConnection.clientId()))
                };
                this.sendCommand(CLIENT, clientTrackingArgs);
                break;
            case BROADCASTING:
                throw new UnsupportedOperationException("Broadcasting mode is not yet supported");
        }
    }

    private void setupInvalidationPubSub() {
        JedisPubSub pubSubInstance = createPubSubInstance();
        Runnable runnable = () -> {
            try {
                // blocking
                invalidationConnection.subscribe(pubSubInstance, INVALIDATION_CHANNEL);
            } catch (Exception eX){
                LOGGER.error("[CACHED_JEDIS_EXCEPTION] Invalidation connection threw exception", eX);
            }
            cachingEnabled = false;
            invalidationConnection.quit();
            invalidationConnection.close();
            cache.cleanUp();
        };
        Thread thread = new Thread(runnable);
        thread.setName("JEDIS_INVALIDATION_CONNECTION_" + clientId);
        thread.start();
    }

    private JedisPubSub createPubSubInstance() {
        return new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                String poisonMessage = POISON_PILL + clientId;
                if(channel.equals(INVALIDATION_CHANNEL) && poisonMessage.equals(message)) {
                    unsubscribe(INVALIDATION_CHANNEL);
                }
            }

            @Override
            public void onMessage(String channel, List<Object> message) {
                for (Object instance : message) {
                    invalidateCache(String.valueOf(instance));
                }
            }

        };
    }

    private void pushPoisonPill() {
        //unsubscribing the invalidation channel
        this.publish(INVALIDATION_CHANNEL , POISON_PILL + clientId);
    }
}

