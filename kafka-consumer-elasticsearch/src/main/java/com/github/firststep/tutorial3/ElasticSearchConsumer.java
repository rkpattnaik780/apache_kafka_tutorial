package com.github.firststep.tutorial3;

import org.apache.http.HttpHost;
//import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient(){
        String hostname = "localhost";

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 9200,"http")
        );

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        // String topic = "twitter_tweets";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto-commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }

    public static void main(String args[]) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        //String jsonString = "{\"foo\" : \"bar\"}";



        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        while(true){
            // consumer.poll(100); // new in kafka, poll is deprecated
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

            logger.info("Received " + records.count() + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord record: records){
                // Where we insert data into ElasticSearch
                // record.value();

                // Kafka generic ID
                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

//                IndexRequest indexRequest = new IndexRequest("twitter","tweets").source(jsonString, XContentType.JSON);

                IndexRequest indexRequest = new IndexRequest("tweets");
                indexRequest.id(id);

                indexRequest.source(record.value().toString(), XContentType.JSON);

                bulkRequest.add(indexRequest); // we add to our bulk request

//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                 logger.info("Data inserted");
                 logger.info(id);
                try{
                    Thread.sleep(10);
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }

            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT); // faster than index req/res

            logger.info("Commiting offsets ....");
            consumer.commitSync();
            logger.info("Offsets have been commited");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // close the client gracefully

        // client.close();


    }
}
