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
    private final int left = 97;
    private final int right = 122;
    private final Random srandom = new Random();
    private final Random irandom = new Random();
    private final Map<Integer, Integer> partitionSize = new HashMap<>();

    public FeedbackProducer() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL +
                ":" +
                KafkaProperties.KAFKA_SERVER_PORT);
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "FeedbackProducer");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<Integer, String>(prop);
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
                        if (record != null)
                            partitionSize.compute(record.partition(), (k, v) -> v == null ?
                                                record.serializedValueSize() :
                                                v + record.serializedKeySize());
                        else exception.printStackTrace();
                    });
            recordSent++;
        }
        System.out.println("Message sent complete.");
        System.out.printf("Sent %d records for 10 minutes.%n", recordSent);
        for (Map.Entry<Integer, Integer> ps : partitionSize.entrySet()) {
            System.out.printf("Partition %d receives %d bytes of data.%n", ps.getKey(), ps.getValue());
        }
    }

    private String getNextMessage() {
        return srandom.ints(left, right+1)
                .limit(irandom.nextInt(1000)+1)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

}

