import redis.clients.jedis.CachedJedis;
import redis.clients.jedis.Jedis;

public class testpp {

    public static void main(String args[]){
        CachedJedis cachedJedis = new CachedJedis();
        CachedJedis cachedJedis1 = new CachedJedis();
        cachedJedis1.thread.start();
        Jedis jedis =new Jedis();
        for(int i=0;i<100;i++){
            jedis.set("foo","neww");
        }
    }
}
