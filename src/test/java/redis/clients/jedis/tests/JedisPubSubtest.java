package redis.clients.jedis.tests;


import static org.junit.Assert.assertEquals;


import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Scanner;

import static redis.clients.jedis.Protocol.Command.CLIENT;
public class JedisPubSubtest  {

    @Test
    public void PubSub()
    {

        JedisPoolConfig config = new JedisPoolConfig();

        JedisPool pool = new JedisPool(config,"localhost",6379);

        Jedis JedisSub = pool.getResource(); // Jedis Instance which subscribes the channel

        Jedis JedisData = pool.getResource(); // Jedis instance which used for tracking

        String ClientID = String.valueOf(JedisSub.clientId());

        //Runnable for tracking the Client and other functionalities
        Runnable RunnableTracker = () -> {
            JedisData.sendCommand(CLIENT, SafeEncoder.encode("TRACKING"),SafeEncoder.encode("on"),
                    SafeEncoder.encode("REDIRECT"),SafeEncoder.encode(ClientID));
            JedisData.set("foo","bar");
            String value = JedisData.get("foo");
            assertEquals("bar",value);
        };

        //Runnable for running the PubSub channel on different thread
        Runnable RunnableSub = () -> {

            JedisPubSub jedisPubSub = new JedisPubSub() {
                //Overriding the different actions taken on the channel
                @Override
                public void onMessage(String channel, List <String> message) {
                    System.out.println("Channel " + channel + " has sent a message : " + message);
                    assertEquals("foo",message.get(0)); //First invalidation message
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


        Thread thread1 = new Thread(RunnableSub);
        Thread thread2 = new Thread(RunnableTracker);
        thread1.start();
        thread2.start();
        PubSubExtended();

    }

    //Another Instance changing the value of key foo
    @Test
    public void PubSubExtended()
    {
        Jedis jedis = new Jedis("localhost");
        jedis.set("foo","newfoo");
    }


}



