import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.Command.GET;

public class test {
    public static void main(String args[])
    {
        Jedis jedis = new Jedis("localhost");

        jedis.sendCommand(GET, SafeEncoder.encode("foo"));
        jedis.scriptLoad(SafeEncoder.encode("return redis.call('set','foo','bar')"));
        System.out.println(jedis.get("foo"));
      //  jedis.sendCommand("SET ")
        Jedis jedis1 = new Jedis("localhost");
        Jedis jedis2 = new Jedis("localhost");
        //jedis.set("foo", "bar");
        //String value = jedis.get("foo");
        //System.out.println(value);
        long p=jedis.clientId();
        String str="CLIENT TRACKING on REDIRECT "+p;
        String s="SET area yara";
        jedis1.echo(s);
        System.out.println(jedis1.get("area"));
        jedis1.echo(str);
        System.out.println(str);
        System.out.println(p);
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

       // test2(jedis);

      // jedis.subscribe(jedisPubSub, "__redis__:invalidate");
       // System.out.println("not coming");
       // jedis.set("foo","bara");
        //String str1=jedis1.get("foo");
        //System.out.println(str1);
        //jedis2.get("foo");
        //jedis2.set("foo","harry");


    }

}
