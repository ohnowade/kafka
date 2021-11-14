package org.apache.kafka.common;

public class FeedbackQueue {
    // TODO: find efficient way to locate a partition by index,
    //  move partitions from the top queue to the bottom queue,
    //  and set bottom queue to top queue (O(1) for all!!)

    // TODO: the queues should be of type List<>,
    //  there should be a mapping relation between a partition's number and its counter,
    //  and also a mapping relation between a partition's number and its index
    //
}
