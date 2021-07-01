package redis.clients.jedis;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static redis.clients.jedis.Protocol.Command.CLIENT;

public class CacheJedis extends Jedis {
    private volatile Jedis jedisInvalidationSubscribe; // Jedis instance for receiving invalidation messages
    private final static String invalidationChannel = "__redis__:invalidate";
    private int maxSize = 100;  // Cache max size by default 100
    private int expireAfterAccess = 5 ; // By default 5 minutes of cache to invalidate a key after access
    private int expireAfterWrite = 5 ; // By default 5 minutes of cache to invalidate a key after write
    private Cache<String , Object> cache;
    private volatile boolean invalidationChnannelBroken = false;
    private Object dummyObject = new Object();
    private JedisPubSub jedisPubSub;

    //Different Constructors present in the jedis class implemented in CacheJedis
    public CacheJedis() {
        super();
        jedisInvalidationSubscribe = new Jedis();
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String uri) {
        super(uri);
        jedisInvalidationSubscribe = new Jedis(uri);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(HostAndPort hp) {
        super(hp);
        jedisInvalidationSubscribe = new Jedis(hp);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(HostAndPort hp, JedisClientConfig config) {
        super(hp, config);
        jedisInvalidationSubscribe = new Jedis(hp, config);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port) {
        super(host, port);
        jedisInvalidationSubscribe = new Jedis(host, port);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, boolean ssl) {
        super(host, port, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, ssl);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int timeout) {
        super(host, port, timeout);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl) {
        super(host, port, timeout, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout, ssl);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout) {
        super(host, port, connectionTimeout, soTimeout);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl) {
        super(host, port, connectionTimeout, soTimeout, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, ssl);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(JedisShardInfo shardInfo) {
        super(shardInfo);
        jedisInvalidationSubscribe = new Jedis(shardInfo);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri) {
        super(uri);
        jedisInvalidationSubscribe = new Jedis(uri);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri, int timeout) {
        super(uri, timeout);
        jedisInvalidationSubscribe = new Jedis(uri, timeout);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri, int timeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout) {
        super(uri, connectionTimeout, soTimeout);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, int infiniteSoTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(URI uri, JedisClientConfig config) {
        super(uri, config);
        jedisInvalidationSubscribe = new Jedis(uri, config);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory) {
        super(jedisSocketFactory);
        jedisInvalidationSubscribe = new Jedis(jedisSocketFactory);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory, JedisClientConfig clientConfig) {
        super(jedisSocketFactory, clientConfig);
        jedisInvalidationSubscribe = new Jedis(jedisSocketFactory, clientConfig);
        clientTracking();
        subscribeInvalidationChannel();
        LoadCache();
    }

    //TODO set various parameters as per the user
    public void LoadCache() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES)
                .expireAfterWrite(expireAfterWrite,TimeUnit.MINUTES)
                .build();
    }

    // TODO : If we want to have cache writes , we should have NOLOOP with Broadcasting Mode @see https://redis.io/topics/client-side-caching
    private void clientTracking() {
        this.sendCommand(CLIENT , SafeEncoder.encode("TRACKING") , SafeEncoder.encode("on") ,
                SafeEncoder.encode("REDIRECT") , SafeEncoder.encode(String.valueOf(jedisInvalidationSubscribe.clientId()))); //CLIENT TRACKING REDIRECT ON clientId
    }

    //Subscribing the pubSub channel in separate thread so that it is non blocking
    public void subscribeInvalidationChannel() {
        startJedisPubSub();
        Runnable runnable = () -> {
                try {
                    jedisInvalidationSubscribe.subscribe(jedisPubSub, invalidationChannel);
                } catch (JedisConnectionException j){
                    //Caught JedisConnectionException CacheJedis will work as normal Jedis
                    //Will no more use caching functionality
                    invalidationChnannelBroken = true;
                    jedisInvalidationSubscribe.close();
                    cache.cleanUp();
                }
        };
        Thread threadSub =new Thread(runnable);
        threadSub.setName("subscribeInvalidationChannelThread");
        threadSub.start();
    }

    public long getCacheSize(){ return cache.size(); }

    private void putInCache(String key , Object value) {
        if(!invalidationChnannelBroken){
            cache.put(key,value);
        }
    }
    private Object getFromCache(String key){
        if(invalidationChnannelBroken){
            return null;
        } else{
            return cache.getIfPresent(key);
        }
    }
    @Override
    public String get(String key) {
        Object value = getFromCache(key);
        if (value != null) {
            //Found in cache
            if(value == dummyObject){
                return null;
            }else {
                return String.valueOf(value);
            }
        } else {//Getting from server
            String valueServer = super.get(key);
            if (valueServer == null){
                putInCache(key,dummyObject);
            } else {
                putInCache(key,valueServer);
            }
            return valueServer;
        }
    }

    @Override
    public List<String> mget(String... keys) {
        List<String> finalValue = new ArrayList<>();
        List<String> keysNotInCache = new ArrayList<>();
        for(int index = 0 ; index < keys.length ; index++){
            Object valueCache = getFromCache(keys[index]);
            if(valueCache != null){
                //Directly get the result if available from the cache
                finalValue.add(String.valueOf(valueCache));
            }else{
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
        if(keysNotInCacheArray.length != 0) {
            //Calling from the server
            valuesFromServer = super.mget(keysNotInCacheArray);
            int indexValuesFromServer = 0;
            for (int index = 0; index < keys.length; index++) {
                if (finalValue.get(index) == null) {
                    //Adding the values returned from server in the null places to maintain order
                    String valueAtindex = valuesFromServer.get(indexValuesFromServer);
                    finalValue.set(index , valueAtindex);
                    if(valueAtindex == null){
                        putInCache(keys[index] , dummyObject);
                    } else {
                        putInCache(keys[index] , valuesFromServer.get(indexValuesFromServer));
                    }
                    indexValuesFromServer++;
                }
            }
        }
        for(int index = 0 ; index < finalValue.size() ; index++){
            if(finalValue.get(index) != null && finalValue.get(index).equals(String.valueOf(dummyObject))){
                finalValue.set(index,null);
            }
        }
        return finalValue;
    }

    public Boolean boolGet(String key) {
        if(getFromCache(key) != null) {
            return true;
        } else {
            String value = super.get(key);
            if(value != null) {
                putInCache(key , value);
            } else{
                putInCache(key , dummyObject);
            }
            return false;
        }
    }

    //Closing the instances jedisPubSub unsubscribe also closes the jedisSub instance
    @Override
    public void close() {
        super.close();
        cache.cleanUp();
        //TODO jedisPubSub.unsubscribe("__redis__:invalidate"); can use this if unsubscribe is thread safe
        jedisInvalidationSubscribe.close();
        //This will throw an exception in runnable which will unsubscribe jedisPubSub and will close the instance as well
    }

    public void startJedisPubSub()
    {
        jedisPubSub = new JedisPubSub() {
            //Overriding different methods of pub sub to take appropriate actions
            @Override
            public void onMessage(String channel, List<Object> message) {
                //TODO Remove this message in the final Commit

                // System.out.println("Channel " + channel + " has sent a message : " + message);
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

