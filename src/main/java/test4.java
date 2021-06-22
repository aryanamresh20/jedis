import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;
import java.util.*;

public class test4 {

    public static void main(String args[])
    {
        Jedis j = new Jedis("localhost");
        Jedis j1=new Jedis("localhost");

        //byte[] str1=jedis.scriptLoad(SafeEncoder.encode("return redis.call('set',foo,'bar')"));
       // jedis.evalsha(str1);
      //  System.out.println(str1);
      // System.out.println(jedis.get("foo"));
        //call
 //System.out.println(jedis.ping());
 //String str1 ="return redis.call(‘set’,KEYS[1],‘bar’)"; // The value of the set key k1 is bar
 ///Object eval1 = jedis.eval(str1, 1,"k1");
 //System.out.println(eval1);
 //System.out.println(jedis.get("k1"));//View execution
        long p=j.clientId();
      //  j.set("foo","bara");
        String str="CLIENT TRACKING on REDIRECT 18";
        System.out.println(str);
        Scanner s=new Scanner(System.in);String query=str;
        String[] q=query.split(" ");
        String cmd='\''+q[0]+'\'';
        for(int i=1;i<q.length;i++)
            cmd+=",\'"+q[i]+'\'';
        j.eval(SafeEncoder.encode("return redis.call("+cmd+")"));
      //  System.out.println(j.eval("return redis.call("+cmd+")"));
        System.out.println(j.get("foo"));
    }

}
