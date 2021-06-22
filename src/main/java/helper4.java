public class helper4 {
    public static void main(String args[]) {


        CacheJedis c = new CacheJedis();
        System.out.println(5);
        c.set("foo","newbar");
        System.out.println(c.get("foo"));
    }
}
