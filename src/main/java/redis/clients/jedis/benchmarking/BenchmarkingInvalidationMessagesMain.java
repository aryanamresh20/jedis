package redis.clients.jedis.benchmarking;

import java.util.Properties;

public class BenchmarkingInvalidationMessagesMain {

    public static void main(String[] args) {
        try {
            Properties props = BenchmarkingUtil.loadConfigFile(args);

            //Assigning various properties to local parameters
            String hostName = props.getProperty("hostName");
            int portNumber = Integer.parseInt(props.getProperty("portNumber"));
            long numberOfKeys = Long.parseLong(props.getProperty("numberOfKeys"));
            long messageSize = Long.parseLong(props.getProperty("messageSize"));
            BenchmarkingUtil.populateKeys(hostName, portNumber, numberOfKeys, messageSize);

            BenchmarkingInvalidationMessages benchmarkingInvalidationMessages = new BenchmarkingInvalidationMessages(hostName ,
                    portNumber , numberOfKeys);

            benchmarkingInvalidationMessages.getInvalidationLatency();

        } catch (Exception ex) {
            System.out.println("FAILED !!!!" + ex);
        }
    }
}
