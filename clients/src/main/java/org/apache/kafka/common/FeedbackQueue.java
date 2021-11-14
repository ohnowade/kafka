package org.apache.kafka.common;

import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FeedbackQueue {
    /**
     * the top queue containing the available partition numbers to be chosen by the partitioner
     **/
    private List<Integer> topQueue;

    /**
     * the bottom queue containing the number of the partitions that use up their allotment, and
     * it becomes the topQueue when the topQueue is empty
     * */
    private List<Integer> bottomQueue;

    private Map<Integer, Integer> counter;

    private int prevPartition;
    private int prevPartitionIndex;

    private final int allotment;

    private final Lock lock = new ReentrantLock();

    public FeedbackQueue(int allotment, List<PartitionInfo> availablePartitions) {
        topQueue = new ArrayList<>();
        bottomQueue = new ArrayList<>();
        counter = new HashMap<>();
        prevPartition = -1;
        prevPartitionIndex = -1;
        for (PartitionInfo partitionInfo : availablePartitions) {
            topQueue.add(partitionInfo.partition());
            counter.put(partitionInfo.partition(), 0);
        }
        this.allotment = allotment;
    }

    public int nextPartition(int recordSize) {
        lock.lock();
        System.out.println("Getting next partition.");
        int rs;
        if (prevPartition < 0) {
            System.out.println("Feedback Queue just initialized.");
            Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
            prevPartitionIndex = random % topQueue.size();
            prevPartition = topQueue.get(prevPartitionIndex);
        } else if (counter.get(prevPartition) >= allotment) {
            System.out.printf("Partition %d used up its allotment. ", prevPartition);
            bottomQueue.add(prevPartition);
            counter.put(prevPartition, 0);
            int size = topQueue.size();
            if (size == 1) {
                topQueue = bottomQueue;
                bottomQueue = new ArrayList<>();
            } else {
                topQueue.set(prevPartitionIndex, topQueue.get(size-1));
                topQueue.remove(size-1);
            }
            Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
            prevPartitionIndex = random % topQueue.size();
            prevPartition = topQueue.get(prevPartitionIndex);
            System.out.printf("The next partition chosen is %d.\n", prevPartition);
        }
        counter.compute(prevPartition, (k, v)->v+recordSize);
        rs = prevPartition;
        System.out.printf("Partition %d is chosen with %d bytes assigned to it.\n", rs, recordSize);
        lock.unlock();
        return rs;
    }

    public boolean hasAvailablePartitions() {
        return topQueue.isEmpty();
    }

    /**
     * Reset the feedback queue when cluster is updated
     * */
    public void reset(List<PartitionInfo> availablePartitions) {
        lock.lock();
        topQueue.clear();
        bottomQueue.clear();
        counter.clear();
        prevPartition = -1;
        prevPartitionIndex = -1;
        for (PartitionInfo partitionInfo : availablePartitions) {
            topQueue.add(partitionInfo.partition());
            counter.put(partitionInfo.partition(), 0);
        }
        lock.unlock();
    }

}
