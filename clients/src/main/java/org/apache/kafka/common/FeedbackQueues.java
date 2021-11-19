/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common;

import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FeedbackQueues {

    /**
     * the unique instance under Singleton design pattern
     */
    private static final FeedbackQueues instance = new FeedbackQueues();

    /**
     * stores the feedback queue of each topic
     */
    private final Map<String, FeedbackQueue> feedbackQueueByTopic = new HashMap<>();

    /**
     * current metadata version number to be matched by each partition in store
     */
    private int version;

    private static int allotment = 32 * 1024;

    private final Lock lock = new ReentrantLock();

    /**
     * Retrieves the unique instance
     * @return the unique instance
     */
    public static FeedbackQueues getInstance() {
        return FeedbackQueues.instance;
    }

    /**
     * Set the allotment of each partition.
     * @param configAllotment the allotment to be set.
     */
    public static void setAllotment(int configAllotment) {
        allotment = configAllotment;
    }

    /**
     * Updates the version number of the partitions under the topic. Initializes the partition in
     * its corresponding queue if it is not present.
     * @param partitions the list of all partition information
     * @param currentVersion current metadata version
     */
    public synchronized void updatePartitions(List<MetadataResponse.PartitionMetadata> partitions,
                                                    int currentVersion) {
            for (MetadataResponse.PartitionMetadata partition : partitions) {
                feedbackQueueByTopic.computeIfAbsent(partition.topic(), k ->
                        new FeedbackQueue()).updatePartition(partition.partition(), currentVersion);
            }
            this.version = currentVersion;
    }

    /**
     * Retrieve the partition to be chosen for current record
     * @param topic the topic this record is dedicated to
     * @param recordSize the serialized size of the record
     * @return the partition chosen for this record
     */
    public synchronized int getPartition(String topic, int recordSize) {
        if (!feedbackQueueByTopic.containsKey(topic)) {
            System.out.printf("Topic %s does not exist!%n", topic);
            return -1;
        }
        return feedbackQueueByTopic.get(topic).nextPartition(recordSize);
    }

    /**
     * This class represents the feedback queue of each topic
     */
    private class FeedbackQueue {

        /**
         * the top queue containing the available partition numbers to be chosen by the partitioner
         **/
        private List<Integer> topQueue;

        /**
         * the bottom queue containing the number of the partitions that use up their allotment, and
         * it becomes the topQueue when the topQueue is empty
         */
        private List<Integer> bottomQueue;

        private Map<Integer, Integer> counter;

        private Map<Integer, Integer> versions;

        private int prevPartition;
        private int prevPartitionIndex;

        public FeedbackQueue() {
            topQueue = new ArrayList<>();
            bottomQueue = new ArrayList<>();
            counter = new HashMap<>();
            versions = new HashMap<>();
            prevPartition = -1;
            prevPartitionIndex = -1;
        }

        private void updatePartition(int partition, int currentVersion) {
            if (!versions.containsKey(partition) || versions.get(partition) < version) {
                // the partition is new, initializes it in the counter and the queue
                counter.put(partition, 0);
                topQueue.add(partition);
            }
            versions.put(partition, currentVersion);
        }

        private int nextPartition(int recordSize) {
                System.out.println("Getting next partition.");
                if (prevPartition < 0) {
                    System.out.println("Feedback Queue just initialized.");
                    prevPartitionIndex = selectAndClean();
                    prevPartition = prevPartitionIndex >= 0 ? topQueue.get(prevPartitionIndex) : -1;
                } else if (counter.get(prevPartition) >= allotment) {
                    System.out.printf("Partition %d used up its allotment. ", prevPartition);
                    bottomQueue.add(prevPartition);
                    counter.put(prevPartition, 0);
                    removeFromTopQueue(prevPartitionIndex);
                    if (topQueue.size() == 0) {
                        topQueue = bottomQueue;
                        bottomQueue = new ArrayList<>();
                    }
                    prevPartitionIndex = selectAndClean();
                    prevPartition = prevPartitionIndex >= 0 ? topQueue.get(prevPartitionIndex) : -1;
                }
                counter.compute(prevPartition, (k, v) -> v + recordSize);
                System.out.printf("Partition %d is chosen with %d bytes assigned to it. Allotment Usage: %d bytes.%n",
                        prevPartition, recordSize, counter.get(prevPartition));
                return prevPartition;
        }

        private int selectAndClean() {
            while (topQueue.size() > 0) {
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                int idx = random % topQueue.size();
                int partition = topQueue.get(idx);
                if (versions.get(partition) < version) {
                    removeFromTopQueue(idx);
                } else {
                    return idx;
                }
            }
            return -1;
        }

        private void removeFromTopQueue(int idx) {
            int size = topQueue.size();
            topQueue.set(idx, topQueue.get(size - 1));
            topQueue.remove(size - 1);
        }
    }
}
