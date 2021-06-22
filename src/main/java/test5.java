import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class test5 {
    public static void main(String args[])
    {
        Jedis jedis=new Jedis("localhost");
        System.out.println(jedis.clientId());
        JedisPubSub jedisPubSub = new JedisPubSub()
        {
            @Override
            public void onMessage(String channel, String message) {
                System.out.println("Channel " + channel + " has sent a message : " + message );
                if(channel.equals("C1")) {
                    /* Unsubscribe from channel C1 after first message is received. */
                    unsubscribe(channel);
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                System.out.println("Client is Subscribed to channel : "+ channel);
                System.out.println("Client is Subscribed to "+ subscribedChannels + " no. of channels");
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                System.out.println("Client is Unsubscribed from channel : "+ channel);
                System.out.println("Client is Subscribed to "+ subscribedChannels + " no. of channels");
            }

        };
        jedis.subscribe(jedisPubSub, "__redis__:invalidate");
    }

}
