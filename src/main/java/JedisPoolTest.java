import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisPoolTest {

    private static final ExecutorService pool = Executors.newCachedThreadPool();

    private static final CountDownLatch latch = new CountDownLatch(20);

    private static final JedisPool jPool = new JedisPool("localhost", 6379);

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        for(int i=0;i<20;i++){
            pool.execute(new RedisTest());
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - start);
        pool.shutdownNow();

    }

    static class RedisTest implements Runnable{

        @Override
        public void run() {
            Jedis jedis = jPool.getResource();
            int i = 1000;
            try{
                while(i-->0){
                    jedis.set("hello", "world");
                }
            }finally{
                jedis.close();
                latch.countDown();
            }
        }

    }

}