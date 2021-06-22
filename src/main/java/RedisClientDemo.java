

import java.io.IOException;
import java.net.Socket;

/**
 * Class: RedisClientDemo
 * version: JDK 1.8
 * create: 2019-10-13 16:48:33
 * @author: heynn
 * Write a redis client imitating Jedis
 */
public class RedisClientDemo {

    private Socket socket;

    public RedisClientDemo(String host, int port) throws IOException {
        // Establish a TCP connection with the server
        this.socket = new Socket(host, port);
    }

    /**
     * set key value
     */
    private String set(String key, String value) throws IOException {
        // Redis communication protocol: RESP, set request packet
        StringBuilder request = new StringBuilder();
        // Request content, line break segmentation, server identification, there are several parts: 3
        request.append("*3").append("\r\n");
        // part 1
        request.append("$3").append("\r\n");// Data length: 3
        request.append("SET").append("\r\n");//Data content: SET
        // part 2
        request.append("$").append(key.getBytes().length).append("\r\n");// Data length: key.getBytes().length
        request.append(key).append("\r\n");//Data content: key
        // part 3
        request.append("$").append(value.getBytes().length).append("\r\n");// Data length: alue.getBytes().length
        request.append(value).append("\r\n");//Data content: value

        String sendContent = request.toString();
        System.out.println("Content sent to server:\n"+sendContent);

        // send to server
        socket.getOutputStream().write(sendContent.getBytes());

        // receive return
        byte[] response = new byte[1024];
        socket.getInputStream().read(response);

        return new String(response);
    }

    /**
     * get key
     */
    private String get(String key) throws IOException {
        // See set() for comments
        StringBuilder request = new StringBuilder();
        request.append("*2").append("\r\n");
        request.append("$3").append("\r\n");
        request.append("GET").append("\r\n");
        request.append("$").append(key.getBytes().length).append("\r\n");
        request.append(key).append("\r\n");
        String sendContent = request.toString();
        socket.getOutputStream().write(sendContent.getBytes());
        byte[] response = new byte[1024];
        socket.getInputStream().read(response);
        return new String(response);
    }

    /**
     * Test
     */
    public static void main(String[] args) throws Exception {
        RedisClientDemo redis = new RedisClientDemo("localhost", 6379);
        String res = redis.set("key_1","value_1");
        System.out.println(res);
        String val = redis.get("key_1");
        System.out.println(val);
    }
}
