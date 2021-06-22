import redis.clients.jedis.Jedis;

public class helper {
    public static void main(String args[])
    {
        Jedis jedis=new Jedis("localhost");
        System.out.println(jedis.get("kumar"));
        jedis.set("kumar","sonu");

    }

}
