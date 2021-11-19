package kafka.examples;

public class FeedbackPartitionerTest {
    public static void main(String[] args) {
        System.out.println("Feedback Partitioner test starts.");
        FeedbackProducer producer = new FeedbackProducer();
        producer.start();
        try {
            producer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Feedback Partitioner test completes.");
    }
}
