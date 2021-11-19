package kafka.examples;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class FeedbackProducer extends Thread{
    private final KafkaProducer<Integer, String> producer;
    private final String topic = "UCLA";
    private final Random srandom = new Random();
    private final Random irandom = new Random();
    private final Map<Integer, Integer> partitionSize = new HashMap<>();
    private final Map<Integer, Integer> recordCount = new HashMap<>();

    public FeedbackProducer(String partitioner, int allotment) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL +
                ":" +
                KafkaProperties.KAFKA_SERVER_PORT);
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "FeedbackProducer");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner);
        prop.put(ProducerConfig.FEEDBACK_QUEUE_ALLOTMENT, allotment);

        producer = new KafkaProducer<>(prop);
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 10 * 60 * 1000;
        int recordSent = 0;
        while (System.currentTimeMillis() < endTime) {
            producer.send(new ProducerRecord<>(topic,
                    null,
                    getNextMessage()),
                    (RecordMetadata record, Exception exception) -> {
                        if (record != null) {
                            partitionSize.compute(record.partition(), (k, v) -> v == null ?
                                    record.serializedValueSize() :
                                    v + record.serializedKeySize());
                            recordCount.compute(record.partition(), (k, v) -> v == null ? 1 : v + 1);
                        }
                        else exception.printStackTrace();
                    });
            recordSent++;
        }
        System.out.println("Message sent complete.");
        System.out.printf("Sent %d records for 10 minutes.%n", recordSent);
        for (int partition : partitionSize.keySet()) {
            System.out.printf("Partition %d receives %d records with %d bytes of data.%n",
                    partition, recordCount.get(partition), partitionSize.get(partition));
        }
    }

    private String getNextMessage() {
        return srandom.ints(97, 123)
                .limit(irandom.nextInt(1000)+1)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

}

