package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static redis.clients.jedis.Protocol.Command.CLIENT;

public class JedisPubSubTest {

    @Test
    public void testServerInvalidationMessage() throws InterruptedException {
        JedisPoolConfig config = new JedisPoolConfig();
        JedisPool pool = new JedisPool(config, "localhost", 6379);
        Jedis jedisSub = pool.getResource(); // Jedis Instance which subscribes the channel
        Jedis jedisData = pool.getResource(); // Jedis instance which used for tracking
        String clientID = String.valueOf(jedisSub.clientId());
        AtomicReference<String> invalidationKey = new AtomicReference<>();

        //Runnable for tracking the Client and other functionalities
        Runnable runnableTracker = () -> {
            jedisData.sendCommand(CLIENT, SafeEncoder.encode("TRACKING"),SafeEncoder.encode("on"),
                    SafeEncoder.encode("REDIRECT"),SafeEncoder.encode(clientID));
            jedisData.set("foo", "bar");
            String value = jedisData.get("foo");
            assertEquals("bar", value);
        };

        //Runnable for running the PubSub channel on different thread
        Runnable runnableSub = () -> {
            JedisPubSub jedisPubSub = new JedisPubSub() {
                //Overriding the different actions taken on the channel
                @Override
                public void onMessage(String channel, List <Object> message) {
                    System.out.println("Channel " + channel + " has sent a message : " + message);
                    invalidationKey.set(String.valueOf(message.get(0))); //First invalidation message
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
            jedisSub.subscribe(jedisPubSub, "__redis__:invalidate");
        };

        Thread thread1 = new Thread(runnableSub);
        Thread thread2 = new Thread(runnableTracker);
        thread1.start();
        thread2.start();

        Jedis jedis = new Jedis("localhost");
        jedis.set("foo", "newfoo");
        Thread.sleep(1);
        assertEquals("foo", invalidationKey.get());
    }
}