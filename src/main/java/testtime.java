public class testtime {
    public static  void main(String args[])
    {
        CacheJedis c=new CacheJedis();
        c.set("ab","cd");
        long st=System.nanoTime();
        for(int i=0;i<1000;i++)
        {
            c.get("ab");
        }
        long la=System.nanoTime();
        System.out.println(la-st);
    }
}
