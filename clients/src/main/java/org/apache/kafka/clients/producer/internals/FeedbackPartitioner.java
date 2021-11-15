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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.FeedbackQueue;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class FeedbackPartitioner implements Partitioner {

    /**
     * Compute the partition for the record
     *
     * @param topic      The topic name
     * @param keyBytes   The serialized key to partition on (or null if no key)
     * @param cluster    The current cluster metadata
     * @param recordSize The size of the given record
     */
    public int partition(String topic, byte[] keyBytes, Cluster cluster, int recordSize) {
        FeedbackQueue fq = cluster.getFeedbackQueueForTopic(topic);
        if (fq.hasAvailablePartitions()) {
            return fq.nextPartition(recordSize);
        }
        System.out.println("No available partitions in Feedback Queue. Randomly choose one.");
        return Utils.toPositive(ThreadLocalRandom.current().nextInt()) %
                cluster.partitionsForTopic(topic).size();
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }
    public void close() {}
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        Partitioner.super.onNewBatch(topic, cluster, prevPartition);
    }
    public void configure(Map<String, ?> configs) {}
}
