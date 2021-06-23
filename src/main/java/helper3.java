public class helper3 {
    public static void main(String args[]) {


        CacheJedis c = new CacheJedis();
        System.out.println(5);
        c.set("foo","bar");
        System.out.println(c.get("foo"));
        c.close();
    }

}
