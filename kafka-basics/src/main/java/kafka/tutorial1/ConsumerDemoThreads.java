package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

    public static void main(String[] args) {
        new ConsumerDemoThreads().run();
    }

    private ConsumerDemoThreads(){

    }

    public void run(){

        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-app";
        String topic = "first_topic_1";

        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerThread = new ConsumerThread(
                topic,
                groupId,
                bootStrapServers,
                latch
        );


//        // Start the thread
//        Thread myThread = new Thread(myConsumerThread);
//        myThread.start();
//
//        // add a shutdown hook
//        Runtime.getRuntime().addShutdownHook(new Thread(
//                () -> {
//                    logger.info("Caught shutdown hook");
//                    ((ConsumerR))
//                }
//        ));

//        try{
//            latch.await();
//        }catch(InterruptedException e){
//            e.printStackTrace();
//        }finally{
//            logger.info("Application is closing");
//        }

    }

        public class ConsumerThread implements Runnable {

            private CountDownLatch latch;
            private KafkaConsumer<String, String> consumer;
            private Logger logger = LoggerFactory.getLogger((ConsumerDemoThreads.class.getName()));

        public ConsumerThread(String topic, String groupId, String bootStrapServers, CountDownLatch latch){
            this.latch = latch;

            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // Subscribe consumer to our topics
            // consumer.subscribe(Collections.singleton); //  we subscribe to single topic
            consumer.subscribe(Arrays.asList("first_topic"));
        }

        @Override
        public void run(){

            try{

                // poll for new data
                while(true){
                    // consumer.poll(100); // new in kafka, poll is deprecated
                    ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord record: records){
                        logger.info("Key : " + record.key() + ", Value: " + record.value());
                        logger.info("Partition : " + record.partition() + ", offset: " + record.offset());
                    }
                }

            } catch (WakeupException e) {
                logger.info("Received shutdown!");
            } finally {
                consumer.close();
                // tell our main code that we are done with the consumer
                latch.countDown();
            }



        }

        public void shutdown(){
            // Special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }



}
