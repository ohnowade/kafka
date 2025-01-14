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
    private final long[] partitionSize = new long[9];
    private final int[] partitionCount = new int[9];

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
        long endTime = startTime + 60 * 1000;
        int recordSent = 0;
        while (System.currentTimeMillis() < endTime) {
            producer.send(new ProducerRecord<>(topic,
                    null,
                    getNextMessage()),
                    (RecordMetadata record, Exception exception) -> {
                        if (record != null) {
                            partitionSize[record.partition()] += record.serializedValueSize();
                            partitionCount[record.partition()]++;
                        }
                        else exception.printStackTrace();
                    });
            recordSent++;
        }
        System.out.println("Message sent complete.");
        System.out.printf("Sent %d records for 1 minute.%n", recordSent);
        for (int i = 0; i < 9; i++) {
            System.out.printf("Partition %d receives %d records with %d bytes of data.%n",
                    i, partitionCount[i], partitionSize[i]);
        }
    }

    private String getNextMessage() {
        return srandom.ints(97, 123)
                .limit(irandom.nextInt(200)+1)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

}

