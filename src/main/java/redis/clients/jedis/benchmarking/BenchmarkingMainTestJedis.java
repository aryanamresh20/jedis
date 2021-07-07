package redis.clients.jedis.benchmarking;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class BenchmarkingMainTestJedis {

    public static void main(String args[]) throws IOException, InterruptedException {

        for(int i=0;i<2;i++) {
            String filePath = "/Users/aryanamresh/Documents/jedis/out/artifacts/jedis_jar/config"+i+".properties";
            Properties props = new Properties();
            FileInputStream ip = new FileInputStream(filePath);
            props.load(ip);

            //Assigning various properties to local parameters
            String hostName = props.getProperty("hostName");
            int portNumber = Integer.parseInt(props.getProperty("portNumber"));
            long numberOfKeys = Long.parseLong(props.getProperty("numberOfKeys"));
            long numberOfClients = Long.parseLong(props.getProperty("numberOfClients"));
            long readPercentage = Long.parseLong(props.getProperty("readPercentage"));
            long writePercentage = Long.parseLong(props.getProperty("writePercentage"));
            long numberOfOperations = Long.parseLong(props.getProperty("numberOfOperations"));
            long meanOperationTime = Long.parseLong(props.getProperty("meanOperationTime"));
            long expireAfterAccess = Long.parseLong(props.getProperty("expireAfterAccessMillis"));
            long expireAfterWrite = Long.parseLong(props.getProperty("expireAfterWriteMillis"));
            long initialCachePopulate = Long.parseLong(props.getProperty("initialCachePopulationIters"));
            long messageSize = Long.parseLong(props.getProperty("messageSize"));
            long readFromGroup = Long.parseLong(props.getProperty("readFromGroup"));
            double sigmaOperationTime = Double.parseDouble(props.getProperty("sigmaOperationTime"));
            System.out.println("--------------------------------------------------");
            System.out.println("results for config"+i);
            System.out.println();

            JedisLatencies jedisLatencies = new JedisLatencies(hostName,portNumber,numberOfClients,numberOfKeys,
                    readPercentage,writePercentage,numberOfOperations,meanOperationTime,
                    sigmaOperationTime,expireAfterAccess,expireAfterWrite,messageSize,initialCachePopulate,
                    readFromGroup);


            jedisLatencies.getLatencies();

        }
    }
}
