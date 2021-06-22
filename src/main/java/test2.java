import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

public class test2
{
    public static void main(String args[]) {
        JedisPool pool = new JedisPool();
        Jedis jedis1 = pool.getResource();
        Jedis jedis = pool.getResource();
        Jedis jedis2 = pool.getResource();

        System.out.println(jedis1.clientId());
        System.out.println(jedis2.clientId());
        Runnable runnable = () ->
        {
            System.out.println(jedis.clientId());
            jedis.set("acd", "acg");
            String str = jedis.get("acd");
            System.out.println(str);

        };
        Runnable runnable2 = () ->
        {
            System.out.println(jedis.clientId());
            JedisPubSub jedisPubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
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
            jedis.subscribe(jedisPubSub, "__redis__:invalidate");
        };


         Thread thread= new Thread(runnable);
        Thread thread1 = new Thread(runnable2);
         thread.start();
         thread.interrupt();
         System.out.println(thread.isInterrupted());
         //if(thread.isInterrupted())
         //thread1.start();
        // jedis.set("harry","veer");
        // System.out.println(jedis.get("harry"));
    }

}
