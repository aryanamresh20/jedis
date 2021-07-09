package redis.clients.jedis.benchmarking;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCacheConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class JedisLatencies {
    private String hostName;
    private int portNumber;
    private final long reads ;
    private final long writes;
    private final long totalClients ;
    private long totalKeys;
    private long totalOperations;
    private final double sigma ;
    private final long mean ;
    private final long messageSize;
    private long interval;
    private final long readFromGroup;
    private final List<Long> operationsTimeLatencies = new CopyOnWriteArrayList<>();
    public JedisLatencies(String host , int port , long numberOfClients , long numberOfKeys , long readPercentage ,
                          long writePercentage , long numberOfOperations , long meanOperationTime , double sigmaOperationTime,
                          long messageLength , long readFromGroupPercentage) {
        totalClients = numberOfClients;
        reads = readPercentage;
        writes = writePercentage;
        totalKeys = numberOfKeys;
        hostName = host;
        portNumber = port;
        totalOperations = numberOfOperations;
        mean = meanOperationTime;
        sigma = sigmaOperationTime;
        messageSize = messageLength;
        readFromGroup = readFromGroupPercentage;
        interval = totalKeys/totalClients;
        populateDatabase();
        startJedisThreads();
    }
    private void populateDatabase(){
        Jedis jedis = new Jedis(hostName,portNumber);
        for(int i = 0 ; i < totalKeys ; i++){
            //Populate database with multiple keys
            jedis.set(String.valueOf(i) , randomString());
        }
        jedis.close();
    }
    //Starting multiple Jedis instances on multiple threads
    private void startJedisThreads() {
        List<Thread> threadList = new ArrayList<>();
        for(int i = 0 ; i < totalClients ; i ++){
            //Waiting a random time to start each client
            try {
                TimeUnit.MILLISECONDS.sleep(waitTime());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Thread thread =new Thread(runnable);
            threadList.add(thread);
            thread.setName("CLIENT_THREAD "+i);
            thread.start();
        }
        for(int i = 0 ; i < totalClients ; i ++){
            try {
                threadList.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    //returns 0 (set) OR 1 (get) depending upon the requirements of reads and writes
    private int getBinaryRandom() {
        Random rand = new Random();
        int random = rand.nextInt(100);
        if(random < writes){
            return 0;
        }else{
            return 1;
        }
    }
    private String randomString(){
        StringBuilder message = new StringBuilder();
        for(int i = 0 ; i < messageSize ; i++){
            message.append(getRandom());
        }
        return String.valueOf(message);
    }

    //return random key Ids present in the database
    private int getRandom() {
        Random rand = new Random();
        return rand.nextInt((int) totalKeys);
    }
    private boolean getFromGroup() {
        Random rand = new Random();
        int random = rand.nextInt(100);
        if(random<readFromGroup){
            return true;
        } else{
            return false;
        }
    }
    private long waitTime(){
        Random rand = new Random();
        long value = (long) (rand.nextGaussian()*sigma+mean);
        if(value < 0){
            return 0;
        }
        return value;
    }



    //Runnable for each thread
    Runnable runnable = () -> {

        Jedis jedis = new Jedis(hostName, portNumber);

        List<Long> localOperationTimeLatencies = new ArrayList<>();
        int index = Integer.parseInt(Thread.currentThread().getName().substring(14));
        long lowerBoundGroup = index*interval;
        long upperBoundGroup = index*(interval)+interval;
        String clientId = String.valueOf(jedis.clientId());

        for(int i = 0 ; i < totalOperations ; i++) {
            long start; //start time
            long end; //end time
            int randomGetSet = getBinaryRandom(); // GET or SET
            int randomKey;
            if(getFromGroup()){
                randomKey = (int) (Math.random() * (upperBoundGroup - lowerBoundGroup) + lowerBoundGroup);
            }else{
                randomKey = getRandom();
            }
            if (randomGetSet == 0) {
                //SET functionality
                String randomString = randomString();
                start = System.nanoTime();
                jedis.set(String.valueOf(randomKey) , randomString);
                end = System.nanoTime();
            } else {
                //GET functionality
                start = System.nanoTime();
                //Check if the key was accessed from the cache
                String value = jedis.get(String.valueOf(randomKey));
                end = System.nanoTime();
               }
            localOperationTimeLatencies.add(end-start);
            long waitTime = waitTime();
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        operationsTimeLatencies.addAll(localOperationTimeLatencies);
    };
    public void getLatencies(){
        Collections.sort(operationsTimeLatencies);
        long p10Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.1));
        long p20Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.2));
        long p30Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.3));
        long p40Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.4));
        long p50Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.5));
        long p75Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.75));
        long p90Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.90));
        long p95Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.95));
        long p97Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.97));
        long p99Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.99));
        long p995Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.995));
        long p997Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.997));
        long p999Time = operationsTimeLatencies.get((int) (operationsTimeLatencies.size()*0.999));
        System.out.println("Overall Latencies in ms");
        System.out.println("P10 "+p10Time/1000000.0);
        System.out.println("P25 "+p20Time/1000000.0);
        System.out.println("P30 "+p30Time/1000000.0);
        System.out.println("P45 "+p40Time/1000000.0);
        System.out.println("P50 "+p50Time/1000000.0);
        System.out.println("P75 "+p75Time/1000000.0);
        System.out.println("P90 "+p90Time/1000000.0);
        System.out.println("P95 "+p95Time/1000000.0);
        System.out.println("P97 "+p97Time/1000000.0);
        System.out.println("P99 "+p99Time/1000000.0);
        System.out.println("P99.5 "+p995Time/1000000.0);
        System.out.println("P99.7 "+p997Time/1000000.0);
        System.out.println("P99.9 "+p999Time/1000000.0);
        System.out.println("P100 "+operationsTimeLatencies.get(operationsTimeLatencies.size()-1)/1000000.0);
        double average = (operationsTimeLatencies.stream().mapToDouble(d->d).average()).orElse(-1);
        System.out.println("Average Overall latency "+average/1000000.0);
        System.out.println("---------------------------------------------------------------------------");
    }
}
