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

            JedisPubSub jedisPubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    System.out.println("Channel " + channel + " has sent a message : " + message);
                    System.out.println(cache.size());
                    cache.invalidate(message);
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
            jedis_sub.subscribe(jedisPubSub, "__redis__:invalidate");
        };
        Thread thread_sub =new Thread(runnable);
        thread_sub.start();
    }
    @Override
    public String get(String key)
    {
        if(cache.asMap().containsKey(key)) {
            System.out.println("from local cache");
            return String.valueOf(cache.asMap().get(key));
        }
        System.out.println("from the database");
        String val= super.get(key);
        cache.asMap().put(key,val);
        return val;
    }


}
