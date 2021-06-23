import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.commands.*;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.Command.CLIENT;

public class CacheJedis extends Jedis implements JedisCommands, MultiKeyCommands,
        AdvancedJedisCommands, ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands,
        ModuleCommands
{
    final Jedis jedis_sub = new Jedis("localhost");

    final String client_id= String.valueOf(jedis_sub.clientId());

    final LoadingCache<String, Object> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Object>() {
                @Override public Object load(String key) throws Exception {
                    return new Object();
                }
            });

    final JedisPubSub jedisPubSub = new JedisPubSub() {
        @Override
        public void onMessage(String channel, String message) {
            System.out.println("Channel " + channel + " has sent a message : " + message);
            System.out.println(cache.size());
            invalidate(message);
            System.out.println(cache.size());
            if (channel.equals("C1")) {
                /* Unsubscribe from channel C1 after first message is received. */
                unsubscribe(channel);
            }
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            System.out.println("Client is Subscribed to channel : " + channel);
            System.out.println("Client is Subscribed to " + subscribedChannels + " no. of channels");
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            System.out.println("Client is Unsubscribed from channel : " + channel);
            System.out.println("Client is Subscribed to " + subscribedChannels + " no. of channels");
        }

    };

    public CacheJedis()
    {
        super();
        this.sendCommand(CLIENT, SafeEncoder.encode("TRACKING"),SafeEncoder.encode("on"),SafeEncoder.encode("REDIRECT"),SafeEncoder.encode(client_id));
        run_in_thread();
    }

    public void run_in_thread()
    {
        Runnable runnable = () ->
        {
            jedis_sub.subscribe(jedisPubSub, "__redis__:invalidate");
        };
        Thread thread_sub =new Thread(runnable);
        thread_sub.start();
    }
    public boolean cache_contains(String key)
    {
        return cache.asMap().containsKey(key);
    }
    public void invalidate(String key)
    {
        cache.invalidate(key);
    }
    public void cache_put(String key,Object value)
    {
        cache.put(key,value);
    }
    public Object cache_get(String key)
    {
        return cache.asMap().get(key);
    }
    @Override
    public String get(String key)
    {
        if(cache_contains(key)) {
            System.out.println("from local cache");
            return String.valueOf(cache_get(key));
        }
        System.out.println("from the database");
        String value= super.get(key);
        cache_put(key,value);
        return value;
    }

    @Override
    public void close()
    {
        super.close();
        jedisPubSub.unsubscribe("__redis__:invalidate");
    }



}
