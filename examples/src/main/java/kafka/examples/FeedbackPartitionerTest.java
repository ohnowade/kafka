package kafka.examples;

public class FeedbackPartitionerTest {
    public static void main(String[] args) {
        System.out.println("Feedback Partitioner test starts.");
        String partitioner = "org.apache.kafka.clients.producer";
        if (args.length > 0 && args[0].toLowerCase().equals("feedback")) {
            partitioner += ".internals.FeedbackPartitioner";
        } else if (args.length > 0 && args[0].toLowerCase().equals("round-robin")) {
            partitioner += ".RoundRobinPartitioner";
        } else {
            partitioner += ".internals..internals.DefaultPartitioner";
        }
        int allotment = 32 * 1024;
        if (args.length > 1) allotment = Integer.parseInt(args[1]);

        FeedbackProducer producer = new FeedbackProducer(partitioner, allotment);
        producer.start();
        try {
            producer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Feedback Partitioner test completes.");
    }
}