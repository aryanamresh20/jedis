package redis.clients.jedis.tests;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;
import redis.clients.jedis.util.SafeEncoder;

public class JedisPubSubtest extends JedisCommandTestBase {

    @Test
    public void PubSub()
    {
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxTotal(100); // Set the maximum number of connections
        config.setMaxIdle(10); // Set the maximum number of idle connections

        JedisPool pool=new JedisPool(config,"localhost",6379);
        Jedis jedis_sub = pool.getResource();
        Jedis jedis_data= pool.getResource();
        System.out.println(jedis_data.clientId());
        System.out.println(jedis_sub.clientId());
        String c= String.valueOf(jedis_sub.clientId());
        Runnable runnable = () ->
        {
            System.out.println(jedis_data.sendCommand(CLIENT, SafeEncoder.encode("TRACKING"),SafeEncoder.encode("on"),SafeEncoder.encode("REDIRECT"),SafeEncoder.encode(c)));
            jedis_data.set("kumar","shivam");
            System.out.println(jedis_data.get("kumar"));
            //  Scanner sc=new Scanner(System.in);
            // sc.nextInt();

        };
        Runnable runnable2 = () ->
        {

            JedisPubSub jedisPubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, List<String>) {
                    System.out.println("Channel " + channel + " has sent a message : " + message);
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

        Thread thread1 = new Thread(runnable2);
        Thread thread= new Thread(runnable);
        thread1.start();
        thread.start();
    }

}
    }

}
