package redis.clients.jedis;

import static redis.clients.jedis.Protocol.Keyword.MESSAGE;
import static redis.clients.jedis.Protocol.Keyword.PMESSAGE;
import static redis.clients.jedis.Protocol.Keyword.PSUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.PUNSUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.SUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.UNSUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.PONG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.SafeEncoder;

public abstract class JedisPubSub {

  private static final String JEDIS_SUBSCRIPTION_MESSAGE = "JedisPubSub is not subscribed to a Jedis instance.";
  private int subscribedChannels = 0;
  private volatile Client client;

  public void onMessage(String channel, String message) {
  }

  public void onPMessage(String pattern, String channel, String message) {
  }

  public void onSubscribe(String channel, int subscribedChannels) {
  }

  public void onUnsubscribe(String channel, int subscribedChannels) {
  }

  public void onPUnsubscribe(String pattern, int subscribedChannels) {
  }

  public void onPSubscribe(String pattern, int subscribedChannels) {
  }

  public void onPong(String pattern) {

  }

  public void unsubscribe() {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.unsubscribe();
    client.flush();
  }

  public void unsubscribe(String... channels) {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.unsubscribe(channels);
    client.flush();
  }

  public void subscribe(String... channels) {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.subscribe(channels);
    client.flush();
  }

  public void psubscribe(String... patterns) {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.psubscribe(patterns);
    client.flush();
  }

  public void punsubscribe() {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.punsubscribe();
    client.flush();
  }

  public void punsubscribe(String... patterns) {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.punsubscribe(patterns);
    client.flush();
  }

  public void ping() {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.ping();
    client.flush();
  }

  public void ping(String argument) {
    if (client == null) {
      throw new JedisConnectionException(JEDIS_SUBSCRIPTION_MESSAGE);
    }
    client.ping(argument);
    client.flush();
  }

  public boolean isSubscribed() {
    return subscribedChannels > 0;
  }

  public void proceedWithPatterns(Client client, String... patterns) {
    this.client = client;
    client.psubscribe(patterns);
    client.flush();
    process(client);
  }

  public void proceed(Client client, String... channels) {
    this.client = client;
    client.subscribe(channels);
    client.flush();
    process(client);
  }

  private void process(Client client) {

    do {
      List<Object> reply = client.getUnflushedObjectMultiBulkReply();
      final Object firstObj = reply.get(0);
      if (!(firstObj instanceof byte[])) {
        throw new JedisException("Unknown message type: " + firstObj);
      }
      final byte[] resp = (byte[]) firstObj;
      if (Arrays.equals(SUBSCRIBE.getRaw(), resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bchannel = (byte[]) reply.get(1);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        onSubscribe(strchannel, subscribedChannels);
      } else if (Arrays.equals(UNSUBSCRIBE.getRaw(), resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bchannel = (byte[]) reply.get(1);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        onUnsubscribe(strchannel, subscribedChannels);
      } else if (Arrays.equals(MESSAGE.getRaw(), resp)) {
        // System.out.println("got message");
        final byte[] bchannel = (byte[]) reply.get(1);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        final byte[] bmesg;
        System.out.println(strchannel);
        String s2="__redis__:invalidate";
        if(strchannel.equals(s2)) {
          //  System.out.println("got message");
          // final byte[] bmesg = (byte[]) reply.get(2);
          final ArrayList<byte[]> in = (ArrayList<byte[]>) reply.get(2);
          //ArrayList<Byte> in = ...;
          // var listOfArrays = new List<byte[]>();

          // System.out.println("here");
          int n = 0;
          for (int i = 0; i < in.size(); i++) {
            for (int j = 0; j < in.get(i).length; j++)
              n++;
          }
          // int n = in.size();
          //  Object o=in.get(0);
          // System.out.println(o.getClass().getSimpleName());
          bmesg = new byte[n];
          int k = 0;
          for (int i = 0; i < in.size(); i++) {
            for (int j = 0; j < in.get(i).length; j++) {
              bmesg[k] = (in.get(i))[j];
              k++;
            }
          }
        }
        else
        {
          bmesg = (byte[]) reply.get(2);
        }

        // final Object strmesg = reply.get(2);
        // System.out.println(strmesg);
        // System.out.println("got message");

        // System.out.println("got message");
        final String strmesg = (bmesg == null) ? null : SafeEncoder.encode(bmesg);
        //  System.out.println("got message");
        onMessage(strchannel, strmesg);

        // onMessage(strchannel,"redisserver");
      } else if (Arrays.equals(PMESSAGE.getRaw(), resp)) {
        final byte[] bpattern = (byte[]) reply.get(1);
        final byte[] bchannel = (byte[]) reply.get(2);
        final byte[] bmesg = (byte[]) reply.get(3);
        final String strpattern = (bpattern == null) ? null : SafeEncoder.encode(bpattern);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        final String strmesg = (bmesg == null) ? null : SafeEncoder.encode(bmesg);
        onPMessage(strpattern, strchannel, strmesg);
      } else if (Arrays.equals(PSUBSCRIBE.getRaw(), resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bpattern = (byte[]) reply.get(1);
        final String strpattern = (bpattern == null) ? null : SafeEncoder.encode(bpattern);
        onPSubscribe(strpattern, subscribedChannels);
      } else if (Arrays.equals(PUNSUBSCRIBE.getRaw(), resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bpattern = (byte[]) reply.get(1);
        final String strpattern = (bpattern == null) ? null : SafeEncoder.encode(bpattern);
        onPUnsubscribe(strpattern, subscribedChannels);
      } else if (Arrays.equals(PONG.getRaw(), resp)) {
        final byte[] bpattern = (byte[]) reply.get(1);
        final String strpattern = (bpattern == null) ? null : SafeEncoder.encode(bpattern);
        onPong(strpattern);
      } else {
        throw new JedisException("Unknown message type: " + firstObj);
      }
    } while (isSubscribed());

    /* Invalidate instance since this thread is no longer listening */
    this.client = null;
  }

  public int getSubscribedChannels() {
    return subscribedChannels;
  }
}
