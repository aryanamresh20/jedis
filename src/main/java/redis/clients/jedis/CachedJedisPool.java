package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.util.JedisURIHelper;
import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.CachedJedisFactory;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import static redis.clients.jedis.Protocol.Command.CLIENT;

public class CachedJedisPool extends CachedJedisPoolAbstract {
    Jedis jedis11 = new Jedis();
    Jedis jedis22 = new Jedis();
    private static final String INVALIDATION_CHANNEL = "__redis__:invalidate";
    private List<CachedJedis> listp = new ArrayList<>();
    private byte[][] clientTrackingArgsp = new byte[][]{
            SafeEncoder.encode("TRACKING"),
            SafeEncoder.encode("ON"),
            SafeEncoder.encode("REDIRECT"),
            SafeEncoder.encode(String.valueOf(jedis11.clientId())),
            SafeEncoder.encode("BCAST")
    };
    public JedisPubSub pubSubInstance = createPubSubInstance();
    Runnable runnable = () -> {
            jedis11.subscribe(pubSubInstance, INVALIDATION_CHANNEL);

    };
    private JedisPubSub createPubSubInstance() {
        return new JedisPubSub() {

            @Override
            public void onMessage(String channel, List<Object> message) {
                System.out.println(message);
                for (Object instance : message) {
                    for(int i=0 ; i< listp.size() ; i++) {
                        listp.get(i).invalidateCache(String.valueOf(instance));
                    }
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                System.out.println("subscribed");
            }
        };
    }
    public void startCaching(){
        jedis22.sendCommand(CLIENT,clientTrackingArgsp);
        Thread thread = new Thread(runnable);
        thread.start();
    }

    private static final Logger log = LoggerFactory.getLogger(CachedJedisPool.class);

    public CachedJedisPool() {
        this(Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host) {
        this(poolConfig, host, Protocol.DEFAULT_PORT);
    }

    public CachedJedisPool(String host, int port) {
        this(new GenericObjectPoolConfig<CachedJedis>(), host, port);
    }

    /**
     * @param url
     * @deprecated This constructor will not accept a host string in future. It will accept only a uri
     * string. You can use {@link JedisURIHelper#isValid(java.net.URI)} before this.
     */
    @Deprecated
    public CachedJedisPool(final String url) {
        URI uri = URI.create(url);
        if (JedisURIHelper.isValid(uri)) {
            initPool(new GenericObjectPoolConfig<CachedJedis>(), new CachedJedisFactory(uri,
                    Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null));
        } else {
            initPool(new GenericObjectPoolConfig<CachedJedis>(), new CachedJedisFactory(url, Protocol.DEFAULT_PORT,
                    Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null));
        }
    }

    /**
     * @param url
     * @param sslSocketFactory
     * @param sslParameters
     * @param hostnameVerifier
     * @deprecated This constructor will not accept a host string in future. It will accept only a uri
     * string. You can use {@link JedisURIHelper#isValid(java.net.URI)} before this.
     */
    @Deprecated
    public CachedJedisPool(final String url, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        URI uri = URI.create(url);
        if (JedisURIHelper.isValid(uri)) {
            initPool(new GenericObjectPoolConfig<CachedJedis>(), new CachedJedisFactory(uri,
                    Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, sslSocketFactory,
                    sslParameters, hostnameVerifier));
        } else {
            initPool(new GenericObjectPoolConfig<CachedJedis>(), new CachedJedisFactory(url, Protocol.DEFAULT_PORT,
                    Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null,
                    false, null, null, null));
        }
    }

    public CachedJedisPool(final URI uri) {
        this(new GenericObjectPoolConfig<CachedJedis>(), uri);
    }

    public CachedJedisPool(final URI uri, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        this(new GenericObjectPoolConfig<CachedJedis>(), uri, sslSocketFactory, sslParameters,
                hostnameVerifier);
    }

    public CachedJedisPool(final URI uri, final int timeout) {
        this(new GenericObjectPoolConfig<CachedJedis>(), uri, timeout);
    }

    public CachedJedisPool(final URI uri, final int timeout, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        this(new GenericObjectPoolConfig<CachedJedis>(), uri, timeout, sslSocketFactory, sslParameters,
                hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password) {
        this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public CachedJedisPool(final String host, int port, String user, final String password) {
        this(new GenericObjectPoolConfig<CachedJedis>(), host, port, user, password);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     String user, final String password) {
        this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, user, password,
                Protocol.DEFAULT_DATABASE);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String user, final String password) {
        this(poolConfig, host, port, timeout, user, password, Protocol.DEFAULT_DATABASE);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final boolean ssl) {
        this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String user, final String password, final boolean ssl) {
        this(poolConfig, host, port, timeout, user, password, Protocol.DEFAULT_DATABASE, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final boolean ssl,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE, ssl,
                sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port) {
        this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port, final boolean ssl) {
        this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port, final boolean ssl, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, ssl, sslSocketFactory, sslParameters,
                hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port, final int timeout) {
        this(poolConfig, host, port, timeout, null);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port, final int timeout, final boolean ssl) {
        this(poolConfig, host, port, timeout, null, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port, final int timeout, final boolean ssl,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        this(poolConfig, host, port, timeout, null, ssl, sslSocketFactory, sslParameters,
                hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final int database) {
        this(poolConfig, host, port, timeout, password, database, null);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String user, final String password, final int database) {
        this(poolConfig, host, port, timeout, user, password, database, null);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final int database, final boolean ssl) {
        this(poolConfig, host, port, timeout, password, database, null, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String user, final String password, final int database, final boolean ssl) {
        this(poolConfig, host, port, timeout, user, password, database, null, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final int database, final boolean ssl,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        this(poolConfig, host, port, timeout, password, database, null, ssl, sslSocketFactory,
                sslParameters, hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final int database, final String clientName) {
        this(poolConfig, host, port, timeout, timeout, password, database, clientName);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String user, final String password, final int database,
                     final String clientName) {
        this(poolConfig, host, port, timeout, timeout, user, password, database, clientName);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final int database, final String clientName,
                     final boolean ssl) {
        this(poolConfig, host, port, timeout, timeout, password, database, clientName, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String user, final String password, final int database,
                     final String clientName, final boolean ssl) {
        this(poolConfig, host, port, timeout, timeout, user, password, database, clientName, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     int timeout, final String password, final int database, final String clientName,
                     final boolean ssl, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        this(poolConfig, host, port, timeout, timeout, password, database, clientName, ssl,
                sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final String password, final int database,
                     final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        super(poolConfig, new CachedJedisFactory(host, port, connectionTimeout, soTimeout, password,
                database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier));
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
                     final String password, final int database, final String clientName, final boolean ssl,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        this(poolConfig, host, port, connectionTimeout, soTimeout, infiniteSoTimeout, null, password,
                database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final String user, final String password,
                     final int database, final String clientName, final boolean ssl,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        this(poolConfig, host, port, connectionTimeout, soTimeout, 0, user, password, database,
                clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
                     final String user, final String password, final int database, final String clientName,
                     final boolean ssl, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        super(poolConfig, new CachedJedisFactory(host, port, connectionTimeout, soTimeout, infiniteSoTimeout,
                user, password, database, clientName, ssl, sslSocketFactory, sslParameters,
                hostnameVerifier));
    }


    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig) {
        this(poolConfig, Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
    }

    public CachedJedisPool(final String host, final int port, final boolean ssl) {
        this(new GenericObjectPoolConfig<CachedJedis>(), host, port, ssl);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final String password, final int database,
                     final String clientName) {
        super(poolConfig, new CachedJedisFactory(host, port, connectionTimeout, soTimeout, password,
                database, clientName));
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final String user, final String password,
                     final int database, final String clientName) {
        super(poolConfig, new CachedJedisFactory(host, port, connectionTimeout, soTimeout, user, password,
                database, clientName));
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
                     final String user, final String password, final int database, final String clientName) {
        super(poolConfig, new CachedJedisFactory(host, port, connectionTimeout, soTimeout, infiniteSoTimeout,
                user, password, database, clientName));
    }

    public CachedJedisPool(final String host, final int port, final boolean ssl,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        this(new GenericObjectPoolConfig<CachedJedis>(), host, port, ssl, sslSocketFactory, sslParameters,
                hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port, final int connectionTimeout, final int soTimeout, final String password,
                     final int database, final String clientName, final boolean ssl) {
        this(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl,
                null, null, null);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final String host,
                     final int port, final int connectionTimeout, final int soTimeout, final String user,
                     final String password, final int database, final String clientName, final boolean ssl) {
        this(poolConfig, host, port, connectionTimeout, soTimeout, user, password, database,
                clientName, ssl, null, null, null);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final URI uri) {
        this(poolConfig, uri, Protocol.DEFAULT_TIMEOUT);
    }


    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final URI uri,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        this(poolConfig, uri, Protocol.DEFAULT_TIMEOUT, sslSocketFactory, sslParameters,
                hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final URI uri, final int timeout) {
        this(poolConfig, uri, timeout, timeout);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final URI uri,
                     final int timeout, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        this(poolConfig, uri, timeout, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final URI uri,
                     final int connectionTimeout, final int soTimeout) {
        this(poolConfig, uri, connectionTimeout, soTimeout, null, null, null);
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final URI uri,
                     final int connectionTimeout, final int soTimeout, final SSLSocketFactory sslSocketFactory,
                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
        super(poolConfig, new CachedJedisFactory(uri, connectionTimeout, soTimeout, null, sslSocketFactory,
                sslParameters, hostnameVerifier));
    }

    public CachedJedisPool(final GenericObjectPoolConfig<CachedJedis> poolConfig, final URI uri,
                     final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
                     final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                     final HostnameVerifier hostnameVerifier) {
        super(poolConfig, new CachedJedisFactory(uri, connectionTimeout, soTimeout, infiniteSoTimeout, null,
                sslSocketFactory, sslParameters, hostnameVerifier));
    }

    public CachedJedisPool(GenericObjectPoolConfig poolConfig, PooledObjectFactory<CachedJedis> factory) {
        super(poolConfig, factory);
    }

    @Override
    public CachedJedis getResource() {
        CachedJedis cachedJedis = super.getResource();
        listp.add(cachedJedis);
        return cachedJedis;
    }

    @Override
    public void returnResource(final CachedJedis resource) {
        if (resource != null) {
            try {
                resource.resetState();
                returnResourceObject(resource);
            } catch (Exception e) {
                returnBrokenResource(resource);
                log.warn("Resource is returned to the pool as broken", e);
            }
        }
    }

}
