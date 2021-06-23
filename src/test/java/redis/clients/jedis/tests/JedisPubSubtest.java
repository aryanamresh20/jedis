package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;

import static redis.clients.jedis.Protocol.Command.CLIENT;
public class JedisPubSubtest extends JedisCommandTestBase {

    @Test
    public void PubSub()
    {
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxTotal(100); // Set the maximum number of connections
        config.setMaxIdle(10); // Set the maximum number of idle connections
        JedisPool pool = new JedisPool(config,"localhost",6379);

        Jedis JedisSub = pool.getResource(); // Jedis Instance which subscribes the channel
        Jedis JedisData = pool.getResource(); // Jedis instance which used for tracking
        Jedis JedisOther = pool.getResource(); // Jedis instance for changing the data to get an invalidation message

        String ClientID= String.valueOf(JedisSub.clientId());

        //Runnable for tracking the Client and other functionalities
        Runnable RunnableTracker = () -> {
            JedisData.sendCommand(CLIENT, SafeEncoder.encode("TRACKING"),SafeEncoder.encode("on"),
                    SafeEncoder.encode("REDIRECT"),SafeEncoder.encode(ClientID)));
            JedisData.set("foo","bar");
            System.out.println(JedisData.get("bar"));
        };

        //Runnable for running the PubSub channel on different thread
        Runnable RunnableSub = () -> {

            JedisPubSub jedisPubSub = new JedisPubSub() {
                //Overriding the different actions taken on the channel
                @Override
                public void onMessage(String channel, List <String> message) {
                    System.out.println("Channel " + channel + " has sent a message : " + message);
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
            JedisSub.subscribe(jedisPubSub, "__redis__:invalidate");
        };
        Runnable RunnableOther = () -> {
            try
            {
                Thread.sleep()
            }
        }

        Thread thread1 = new Thread(runnable2);
        Thread thread= new Thread(runnable);
        thread1.start();
        thread.start();
    }

}



