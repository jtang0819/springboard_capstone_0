package raw_data_to_kafka_topic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;


public class raw_data_to_kafka_topic {
    Logger logger = LoggerFactory.getLogger(raw_data_to_kafka_topic.class.getName());

    public static void main(String[] args) throws Exception {

        new raw_data_to_kafka_topic().run();
    }

    public void run() throws Exception {
        logger.info("setup");
        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        //loop json files on local computer
        File dir = new File("test_folder");
        File[] directoryListing = dir.listFiles();

        if (directoryListing != null) {
            for (File child : directoryListing) {
                String path = child.getPath();
                String json = readFileAsString(path);
                // loop to send data to kafka topic
                // System.out.println(json);
                if (json != null) {
//                    System.out.println(json);
                    producer.send(new ProducerRecord<>("raw_data", null, json), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.error("Something bad happened:", e);
                            }
                        }
                    });

                }
            }
        }
        logger.info("End of application");
    }

    public KafkaProducer<String, String> createKafkaProducer(){

        // producer properties
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString((Integer.MAX_VALUE)));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer at the expense of latency
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public static String readFileAsString(String file)throws Exception
    {
        return new String(Files.readAllBytes(Paths.get(file)));
    }


}

