import redis.clients.jedis.Jedis;

public class testtime2 {
    public static  void main(String args[])
    {
        Jedis c=new Jedis("localhost");
        c.set("abb","cd");
        long st=System.nanoTime();
        for(int i=0;i<1000;i++)
        {
            c.get("abb");
        }
        long la=System.nanoTime();
        System.out.println(la-st);
    }
}
