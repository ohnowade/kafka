package kafka.examples;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FeedbackPartitionerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.19.157:9092");
        AdminClient adminClient = KafkaAdminClient.create(props);
        List<NewTopic> topicList = new ArrayList<>();
        List<String> topicName = new ArrayList<>();
        topicList.add(new NewTopic("UCLA", 9, (short)1));
        topicName.add("UCLA");
        CreateTopicsResult res = adminClient.createTopics(topicList);
        try {
            res.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Created topic UCLA with 9 partitions and 1 replication.");

        String partitioner = "org.apache.kafka.clients.producer";
        if (args.length > 0 && args[0].toLowerCase().equals("feedback")) {
            partitioner += ".internals.FeedbackPartitioner";
        } else if (args.length > 0 && args[0].toLowerCase().equals("round-robin")) {
            partitioner += ".RoundRobinPartitioner";
        } else {
            partitioner += ".internals.DefaultPartitioner";
        }
        int allotment = 32 * 1024;
        if (args.length > 1) allotment = Integer.parseInt(args[1]);

        FeedbackProducer producer = new FeedbackProducer(partitioner, allotment);

        System.out.println("Feedback Partitioner test starts.");

        producer.start();
        try {
            producer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Feedback Partitioner test completes.");

        DeleteTopicsResult rs = adminClient.deleteTopics(Stream.of("UCLA").collect(Collectors.toList()));
        try {
            rs.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Deleted topic UCLA.");
    }
}
