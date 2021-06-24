package redis.clients.jedis;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static redis.clients.jedis.Protocol.Command.CLIENT;

public class CacheJedis extends Jedis {
    private volatile Jedis jedisInvalidationSubscribe; // Jedis instance for receiving invalidation messages
    private final static String invalidationChannel = "__redis__:invalidate";
    private Cache<String , Object> cache;
    private volatile boolean invalidationChannelBroken = false;
    private boolean enableCaching = false;
    private final Object dummyObject = new Object();
    private JedisPubSub jedisPubSub;
    private String clientID;

    //Different Constructors present in the jedis class implemented in CacheJedis
    public CacheJedis() {
        super();
        jedisInvalidationSubscribe = new Jedis();
    }

    public CacheJedis(String uri) {
        super(uri);
        jedisInvalidationSubscribe = new Jedis(uri);
    }

    public CacheJedis(HostAndPort hp) {
        super(hp);
        jedisInvalidationSubscribe = new Jedis(hp);
    }

    public CacheJedis(HostAndPort hp, JedisClientConfig config) {
        super(hp, config);
        jedisInvalidationSubscribe = new Jedis(hp, config);
    }

    public CacheJedis(String host, int port) {
        super(host, port);
        jedisInvalidationSubscribe = new Jedis(host,port);
    }

    public CacheJedis(String host, int port, boolean ssl) {
        super(host, port, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, ssl);
    }

    public CacheJedis(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(String host, int port, int timeout) {
        super(host, port, timeout);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout);
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl) {
        super(host, port, timeout, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout, ssl);
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout) {
        super(host, port, connectionTimeout, soTimeout);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout);
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl) {
        super(host, port, connectionTimeout, soTimeout, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, ssl);
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(JedisShardInfo shardInfo) {
        super(shardInfo);
        jedisInvalidationSubscribe = new Jedis(shardInfo);
    }

    public CacheJedis(URI uri) {
        super(uri);
        jedisInvalidationSubscribe = new Jedis(uri);
    }

    public CacheJedis(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(URI uri, int timeout) {
        super(uri, timeout);
        jedisInvalidationSubscribe = new Jedis(uri, timeout);
    }

    public CacheJedis(URI uri, int timeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout) {
        super(uri, connectionTimeout, soTimeout);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout);
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, int infiniteSoTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CacheJedis(URI uri, JedisClientConfig config) {
        super(uri, config);
        jedisInvalidationSubscribe = new Jedis(uri, config);
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory) {
        super(jedisSocketFactory);
        jedisInvalidationSubscribe = new Jedis(jedisSocketFactory);
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory, JedisClientConfig clientConfig) {
        super(jedisSocketFactory, clientConfig);
        jedisInvalidationSubscribe = new Jedis(jedisSocketFactory, clientConfig);
    }

    public void enableCaching(CacheConfig cacheConfig){
        enableCaching = true;
        cache = CacheBuilder.newBuilder()
                .maximumSize(cacheConfig.getMaxSize())
                .expireAfterAccess(cacheConfig.getExpireAfterAccess(), cacheConfig.getUnit())
                .expireAfterWrite(cacheConfig.getExpireAfterWrite(), cacheConfig.getUnit())
                .build();
        clientTracking();
        subscribeInvalidationChannel();
    }

    public void enableCaching(){
        enableCaching = true;
        cache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .expireAfterWrite(5,TimeUnit.MINUTES)
                .build();
        clientTracking();
        subscribeInvalidationChannel();
    }



    // TODO : If we want to have cache writes , we should have NOLOOP with Broadcasting Mode @see https://redis.io/topics/client-side-caching
    private void clientTracking() {
        this.sendCommand(CLIENT , SafeEncoder.encode("TRACKING") , SafeEncoder.encode("on") ,
                SafeEncoder.encode("REDIRECT") , SafeEncoder.encode(String.valueOf(jedisInvalidationSubscribe.clientId()))); //CLIENT TRACKING REDIRECT ON clientId
    }

    //Subscribing the pubSub channel in separate thread so that it is non blocking
    public void subscribeInvalidationChannel() {
        clientID = String.valueOf(this.clientId());
        startJedisPubSub();
        Runnable runnable = () -> {
                try {
                    jedisInvalidationSubscribe.subscribe(jedisPubSub, invalidationChannel);
                } catch (JedisConnectionException j){
                    invalidationChannelBroken = true;
                    jedisInvalidationSubscribe.close();
                    cache.cleanUp();
                }
        };
        Thread threadSub =new Thread(runnable);
        threadSub.setName("subscribeInvalidationChannelThread");
        threadSub.start();
    }

    public long getCacheSize() {
        if (enableCaching) {
            return cache.size();
        }else{
            return 0;
        }
    }

    private void putInCache(String key , Object value) {
        if(!invalidationChannelBroken){
            cache.put(key,value);
        }
    }
    private Object getFromCache(String key){
        if(invalidationChannelBroken){
            return null;
        } else{
            return cache.getIfPresent(key);
        }
    }
    @Override
    public String get(String key) {
        checkIsInMultiOrPipeline();
        if(enableCaching) {
            Object value = getFromCache(key);
            if (value != null) {
                //Found in cache
                if (value == dummyObject) {
                    return null;
                } else {
                    return String.valueOf(value);
                }
            } else {//Getting from server
                String valueServer = super.get(key);
                if (valueServer == null) {
                    putInCache(key, dummyObject);
                } else {
                    putInCache(key, valueServer);
                }
                return valueServer;
            }
        }else{
            return super.get(key);
        }
    }

    @Override
    public List<String> mget(String... keys) {
        checkIsInMultiOrPipeline();
        if(enableCaching) {
            List<String> finalValue = new ArrayList<>();
            List<String> keysNotInCache = new ArrayList<>();
            for (int index = 0; index < keys.length; index++) {
                Object valueCache = getFromCache(keys[index]);
                if (valueCache != null) {
                    //Directly get the result if available from the cache
                    finalValue.add(String.valueOf(valueCache));
                } else {
                    //Initializing the key values that would be returned from the server as null
                    finalValue.add(null);
                    //If not in cache add in the parameter send to server
                    keysNotInCache.add(keys[index]);
                }
            }
            String[] keysNotInCacheArray = new String[keysNotInCache.size()];
            //Converting into compatible parameter for mget command
            keysNotInCacheArray = keysNotInCache.toArray(keysNotInCacheArray);
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
                            putInCache(keys[index], dummyObject);
                        } else {
                            putInCache(keys[index], valuesFromServer.get(indexValuesFromServer));
                        }
                        indexValuesFromServer++;
                    }
                }
            }
            for (int index = 0; index < finalValue.size(); index++) {
                if (finalValue.get(index) != null && finalValue.get(index).equals(String.valueOf(dummyObject))) {
                    finalValue.set(index, null);
                }
            }
            return finalValue;
        }else{
            return super.mget(keys);
        }
    }


    @Override
    public Map<String, String> hgetAll(String key) {
        checkIsInMultiOrPipeline();
        if(enableCaching) {
            Object value = getFromCache(key);
            if (value != null) {
                //Casting the Object to map
                Map<String, String> mapValue = Map.class.cast(value);
                //Found in cache
                return mapValue;
            } else {
                //Getting from server
                Map<String, String> valueServer = super.hgetAll(key);
                putInCache(key, valueServer);
                return valueServer;
            }
        }else{
            return super.hgetAll(key);
        }
    }

    public Boolean boolGet(String key) {
        if(enableCaching) {
            if (getFromCache(key) != null) {
                return true;
            } else {
                String value = super.get(key);
                if (value != null) {
                    putInCache(key , value);
                } else {
                    putInCache(key , dummyObject);
                }
                return false;
            }
        }else{
            return false;
        }
    }

    //Closing the instances jedisPubSub unsubscribe also closes the jedisSub instance
    @Override
    public void close() {
        if(enableCaching) {
            this.publish("__redis__:invalidate" , "close" + clientID); //unsubscribing the invalidation channel
            cache.cleanUp();
        }
        super.close();
    }

    public void startJedisPubSub()
    {
        jedisPubSub = new JedisPubSub() {
            //Overriding different methods of pub sub to take appropriate actions

            @Override
            public void onMessage(String channel, String message) {
                String closeMessage = "close"+clientID;
                if(channel.equals(invalidationChannel) && closeMessage.equals(message)) {
                    unsubscribe(invalidationChannel);
                }
            }

            @Override
            public void onMessage(String channel, List<Object> message) {
                //TODO Remove this message in the final Commit

                //System.out.println("Channel " + channel + " has sent a message : " + message);
                for (Object instance : message) {
                    cache.invalidate(String.valueOf(instance));///invalidating the keys received from the channel considering as a List
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                //TODO Remove this message in the final Commit

                // System.out.println("Client is Subscribed to channel : " + channel);
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                //TODO Remove this message in the final Commit

                // System.out.println("Client is Unsubscribed from channel : " + channel);
            }
        };
    }

}

