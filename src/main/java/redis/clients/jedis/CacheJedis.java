package redis.clients.jedis;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import redis.clients.jedis.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static redis.clients.jedis.Protocol.Command.CLIENT;

public class CacheJedis extends Jedis {
    private Jedis jedisInvalidationSubscribe ; // Jedis instance for receiving invalidation messages
    private final static String invalidationChannel = "__redis__:invalidate";
    private int maxSize = 100;  // Cache max size by default 100
    private int expireAfterAccess = 5 ; // By default 5 minutes of cache to invalidate a key after acess
    private int expireAfterWrite = 5 ; // By default 5 minutes of cache to invalidate a key after write
    private LoadingCache<String, Object> cache;
    final JedisPubSub jedisPubSub = new JedisPubSub() {
        //Overriding different methods of pub sub to take appropriate actions
        @Override
        public void onMessage(String channel, List<Object> message) {
            //TODO Remove this message in the final Commit
            System.out.println("Channel " + channel + " has sent a message : " + message);
            for (Object instance : message)
                invalidate(String.valueOf(instance)); //invalidating the keys received from the channel considering as a List
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            //TODO Remove this message in the final Commit
            System.out.println("Client is Subscribed to channel : " + channel);
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            //TODO Remove this message in the final Commit
            System.out.println("Client is Unsubscribed from channel : " + channel);
        }
    };
    //TODO set various parameters as per the user
    public void LoadCache()
    {
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES)
                .expireAfterWrite(expireAfterWrite,TimeUnit.MINUTES)
                .build(new CacheLoader<String, Object>() {
                    @Override public Object load(String key) throws Exception {
                        return new Object();
                    }
                });
    }

    //Different Constructors present in the jedis class implemented in CacheJedis
    public CacheJedis() {
        super();
        jedisInvalidationSubscribe = new Jedis();
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String uri) {
        super(uri);
        jedisInvalidationSubscribe = new Jedis(uri);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(HostAndPort hp) {
        super(hp);
        jedisInvalidationSubscribe = new Jedis(hp);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(HostAndPort hp, JedisClientConfig config) {
        super(hp, config);
        jedisInvalidationSubscribe = new Jedis(hp, config);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port) {
        super(host, port);
        jedisInvalidationSubscribe = new Jedis(host, port);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, boolean ssl) {
        super(host, port, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, ssl);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int timeout) {
        super(host, port, timeout);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl) {
        super(host, port, timeout, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout, ssl);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout) {
        super(host, port, connectionTimeout, soTimeout);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl) {
        super(host, port, connectionTimeout, soTimeout, ssl);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, ssl);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(JedisShardInfo shardInfo) {
        super(shardInfo);
        jedisInvalidationSubscribe = new Jedis(shardInfo);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri) {
        super(uri);
        jedisInvalidationSubscribe = new Jedis(uri);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri, int timeout) {
        super(uri, timeout);
        jedisInvalidationSubscribe = new Jedis(uri, timeout);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri, int timeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout) {
        super(uri, connectionTimeout, soTimeout);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, int infiniteSoTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        jedisInvalidationSubscribe = new Jedis(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(URI uri, JedisClientConfig config) {
        super(uri, config);
        jedisInvalidationSubscribe = new Jedis(uri, config);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory) {
        super(jedisSocketFactory);
        jedisInvalidationSubscribe = new Jedis(jedisSocketFactory);
        clientTracking();
        pubSubThread();
        LoadCache();
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory, JedisClientConfig clientConfig) {
        super(jedisSocketFactory, clientConfig);
        jedisInvalidationSubscribe = new Jedis(jedisSocketFactory, clientConfig);
        clientTracking();
        pubSubThread();
        LoadCache();
    }
    // TODO : If we want to have cache writes , we should have NOLOOP with Broadcasting Mode
    public void clientTracking() {
        this.sendCommand(CLIENT , SafeEncoder.encode("TRACKING") , SafeEncoder.encode("on") ,
                SafeEncoder.encode("REDIRECT") , SafeEncoder.encode(getClientId())); //CLIENT TRACKING REDIRECT ON clientId
    }
    //Subscribing the pubSub channel in separate thread so that it is non blocking
    public void pubSubThread() {
        Runnable runnable = () -> {
            jedisInvalidationSubscribe.subscribe(jedisPubSub, invalidationChannel);
        };
        Thread threadSub =new Thread(runnable);
        threadSub.setName("pubSubThread");
        threadSub.start();
    }

    public String getClientId() { return String.valueOf(jedisInvalidationSubscribe.clientId()); }
    public void invalidate(String key) { cache.invalidate(key); }
    public void cachePut(String key,Object value) { cache.put(key,value); }
    public Object cacheGet(String key) { return cache.getIfPresent(key); }
    public void connection() { if(checkConnect()) cache.cleanUp(); } //if connection lost flush the cache
    public long getCacheSize(){ return cache.size(); }

    @Override
    public String get(String key) {
        if(cacheGet(key)!= null) {
            return String.valueOf(cacheGet(key));
        } else {
            String value=super.get(key);
            if(value!=null)
            cachePut(key,value);
            return value;
        }
    }

    @Override
    public List<String> mget(String... keys) {
        for(int index = 0 ; index < keys.length ; index++){
            keys[index] = get(keys[index]);
        }
        return Arrays.asList(keys);
    }

    //For checking if the connection is active
    public boolean checkConnect() {
        jedisPubSub.ping();
        int flag = jedisPubSub.getSubscribedChannels();
        if(flag == 1)
            return true;
        return false;
    }

    //Closing the instances jedisPubSub unsubscribe also closes the jedisSub instance
    @Override
    public void close() {
        super.close();
        cache.cleanUp();
        jedisPubSub.unsubscribe("__redis__:invalidate");
    }

}

