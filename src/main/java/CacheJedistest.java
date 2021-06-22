public class CacheJedistest {
    public static void main(String args[])
    {
        CacheJedis test=new CacheJedis();
        Thread thread=Thread.currentThread();
        try
        {
            thread.sleep(100);
        }
        catch(InterruptedException ex)
        {

        }
        test.set("foo","bar");
        System.out.println(test.get("foo"));
        System.out.println(test.get("foo"));;
    }
}
