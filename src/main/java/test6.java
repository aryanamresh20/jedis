import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Scanner;

import static redis.clients.jedis.Protocol.Command.*;

public class test6 {
    public static void main(String args[]) {
        Jedis jedis = new Jedis("localhost");
        //jedis.set("foo","abc");
        System.out.println(jedis.sendCommand(SET,SafeEncoder.encode("HOA"),SafeEncoder.encode("MADRID")));
        System.out.println(jedis.get("HOA"));
        System.out.println(jedis.sendCommand(CLIENT,SafeEncoder.encode("TRACKING"),SafeEncoder.encode("on"),SafeEncoder.encode("REDIRECT"),SafeEncoder.encode("3")));
        System.out.println("come");
       Client C=jedis.getClient();
        C.set("poo","foo");
        C.getStatusCodeReply();
        C.get("poo");

            Scanner sc=new Scanner(System.in);
            sc.nextInt();

    }
}
