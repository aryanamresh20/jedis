package redis.clients.jedis;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import redis.clients.jedis.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.List;

import static redis.clients.jedis.Protocol.Command.CLIENT;

public class CacheJedis extends Jedis {
    final Jedis jedisSub = new Jedis(); // Jedis instance for Client tracking
    final String clientId= String.valueOf(jedisSub.clientId());
    final LoadingCache<String, Object> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Object>() {
                @Override public Object load(String key) throws Exception {
                    return new Object();
                }
            });
    final JedisPubSub jedisPubSub = new JedisPubSub() {
        //Overriding different methods of pub sub to take appropriate actions
        @Override
        public void onMessage(String channel, List<Object> message) {
            System.out.println("Channel " + channel + " has sent a message : " + message);
            for (Object instance : message)
                invalidate(String.valueOf(instance)); //invalidating the keys received from the channel considering as a List
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            System.out.println("Client is Subscribed to channel : " + channel);
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            System.out.println("Client is Unsubscribed from channel : " + channel);
        }
    };

    //Different Constructors present in the jedis class implemented in CacheJedis
    public CacheJedis() {
        super();
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String uri) {
        super(uri);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(HostAndPort hp) {
        super(hp);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(HostAndPort hp, JedisClientConfig config) {
        super(hp, config);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port) {
        super(host, port);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, boolean ssl) {
        super(host, port, ssl);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int timeout) {
        super(host, port, timeout);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl) {
        super(host, port, timeout, ssl);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int timeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout) {
        super(host, port, connectionTimeout, soTimeout);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl) {
        super(host, port, connectionTimeout, soTimeout, ssl);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(String host, int port, int connectionTimeout, int soTimeout, int infiniteSoTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(JedisShardInfo shardInfo) {
        super(shardInfo);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri) {
        super(uri);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri, int timeout) {
        super(uri, timeout);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri, int timeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout) {
        super(uri, connectionTimeout, soTimeout);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri, int connectionTimeout, int soTimeout, int infiniteSoTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(URI uri, JedisClientConfig config) {
        super(uri, config);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory) {
        super(jedisSocketFactory);
        clientTracking();
        pubSubThread();
    }

    public CacheJedis(JedisSocketFactory jedisSocketFactory, JedisClientConfig clientConfig) {
        super(jedisSocketFactory, clientConfig);
        clientTracking();
        pubSubThread();
    }
    // TODO : If we want to have cache writes , we should have NOLOOP with Broadcasting Mode
    public void clientTracking() {
        this.sendCommand(CLIENT , SafeEncoder.encode("TRACKING") , SafeEncoder.encode("on") ,
                SafeEncoder.encode("REDIRECT") , SafeEncoder.encode(clientId)); //CLIENT TRACKING REDIRECT ON clientId
    }
    //Subscribing the pubSub channel in separate thread so that it is non blocking
    public void pubSubThread()
    {
        Runnable runnable = () -> {
            jedisSub.subscribe(jedisPubSub, "__redis__:invalidate");
        };
        Thread threadSub =new Thread(runnable);
        threadSub.setName("pubSubThread");
        threadSub.start();
    }

    public boolean cacheContains(String key)
    {
        return cache.asMap().containsKey(key);
    }
    public void invalidate(String key)
    {
        cache.invalidate(key);
    }
    public void cachePut(String key,Object value)
    {
        cache.put(key,value);
    }
    public Object cacheGet(String key)
    {
        return cache.asMap().get(key);
    }
    public void connection() { if(checkConnect()) cache.cleanUp(); } //if connection lost flush the cache
    public long getCacheSize(){ return cache.size(); }

    @Override
    public String get(String key) {
        if( cacheContains(key) ) {
            return String.valueOf(cacheGet(key)); //Directly fetch the value from cache if present
        } else {
            String value = super.get(key); //Fetching value from the server
            cachePut(key, value);
            return value;
        }
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

