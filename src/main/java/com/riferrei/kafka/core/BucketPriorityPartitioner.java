/**

    Copyright © 2020 Ricardo Ferreira (riferrei@riferrei.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/

package com.riferrei.kafka.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import static com.riferrei.kafka.core.Bucket.size;

public class BucketPriorityPartitioner implements Partitioner {

    private Partitioner fallbackPartitioner;
    private BucketPriorityConfig config;
    private ThreadLocal<String> lastBucket;
    private Map<String, Bucket> buckets;
    private int lastPartitionCount;

    @Override
    public void configure(Map<String, ?> configs) {
        config = new BucketPriorityConfig(configs);
        List<Integer> bucketAlloc = new ArrayList<>(config.allocation().size());
        for (String allocItem : config.allocation()) {
            allocItem = allocItem.replaceAll("%", "").trim();
            bucketAlloc.add(Integer.parseInt(allocItem));
        }
        if (config.buckets().size() != bucketAlloc.size()) {
            throw new InvalidConfigurationException("The bucket allocation " + 
                "doesn't match with the number of buckets configured.");
        }
        int oneHundredPerc = bucketAlloc.stream()
            .mapToInt(Integer::intValue)
            .sum();
        if (oneHundredPerc != 100) {
            throw new InvalidConfigurationException("The bucket allocation " +
                "is incorrect. The sum of all buckets needs to be 100.");
        }
        switch (config.fallbackAction()) {
            case DEFAULT:
                fallbackPartitioner = new DefaultPartitioner();
                break;
            case ROUNDROBIN:
                fallbackPartitioner = new RoundRobinPartitioner();
                break;
            default:
        }
        lastBucket = new ThreadLocal<>();
        buckets = new LinkedHashMap<>();
        for (int i = 0; i < config.buckets().size(); i++) {
            String bucketName = config.buckets().get(i).trim();
            buckets.put(bucketName, new Bucket(bucketAlloc.get(i)));
        }
        // Sort the buckets with higher allocation to come
        // first than the others. This will help later during
        // bucket allocation if unassigned partitions are found
        // and therefore can be assigned to the first buckets.
        buckets = buckets.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
        Object value, byte[] valueBytes, Cluster cluster) {
        int partition = -1;
        if (config.topic() != null && config.topic().equals(topic)) {
            if (key instanceof String) {
                String keyValue = (String) key;
                String[] keyValueParts = keyValue.split(config.delimiter());
                if (keyValueParts.length >= 1) {
                    String bucketName = keyValueParts[0].trim();
                    if (buckets.containsKey(bucketName)) {
                        lastBucket.set(bucketName);
                        partition = getPartition(bucketName, cluster);
                    } else {
                        partition = fallback(topic, key, keyBytes,
                            value, valueBytes, cluster);
                    }
                }
            } else {
                partition = fallback(topic, key, keyBytes,
                    value, valueBytes, cluster);
            }
        } else {
            partition = fallback(topic, key, keyBytes,
                value, valueBytes, cluster);
        }
        return partition;
    }

    private int fallback(String topic, Object key, byte[] keyBytes,
        Object value, byte[] valueBytes, Cluster cluster) {
        int partition = -1;
        switch (config.fallbackAction()) {
            case DEFAULT:
                partition = fallbackPartitioner.partition(topic,
                    key, keyBytes, value, valueBytes, cluster);
                break;
            case ROUNDROBIN:
                partition = fallbackPartitioner.partition(topic,
                    key, keyBytes, value, valueBytes, cluster);
                break;
            case DISCARD:
                break;
        }
        return partition;
    }

    private int getPartition(String bucketName, Cluster cluster) {
        int partitionCount = cluster.partitionCountForTopic(config.topic());
        // Check if the # of partitions has changed
        // and trigger an update if that happened.
        if (lastPartitionCount != partitionCount) {
            updateBucketPartitions(cluster);
            lastPartitionCount = partitionCount;
        }
        Bucket bucket = buckets.get(bucketName);
        return bucket.nextPartition();
    }

    private void updateBucketPartitions(Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(config.topic());
        if (partitions.size() < buckets.size()) {
            StringBuilder message = new StringBuilder();
            message.append("The number of partitions available for the topic '");
            message.append(config.topic()).append("' is incompatible with the ");
            message.append("number of buckets. It needs to be at least ");
            message.append(buckets.size()).append(".");
            throw new InvalidConfigurationException(message.toString());
        }
        // Sort partitions in ascendent order
        partitions = partitions.stream()
            .sorted(Comparator.comparing(PartitionInfo::partition))
            .collect(Collectors.toList());
        // Design the layout of the distribution
        int distribution = 0;
        Map<String, Integer> layout = new LinkedHashMap<>();
        for (Map.Entry<String, Bucket> bucket : buckets.entrySet()) {
            int allocation = bucket.getValue().getAllocation();
            int bucketSize = size(allocation, partitions.size());
            layout.put(bucket.getKey(), bucketSize);
            distribution += bucketSize;
        }
        // Check if there are unassigned partitions.
        // If so then distribute them over the buckets
        // starting from the top to bottom until there
        // are no partitions left.
        int remaining = partitions.size() - distribution;
        Iterator<String> iter = buckets.keySet().iterator();
        while (remaining > 0) {
            String bucket = iter.next();
            int bucketSize = layout.get(bucket);
            layout.put(bucket, ++bucketSize);
            remaining--;
        }
        // Finally assign the available partitions to buckets
        int partition = -1;
        TopicPartition topicPartition = null;
        bucketAssign: for (Map.Entry<String, Bucket> bucket : buckets.entrySet()) {
            int bucketSize = layout.get(bucket.getKey());
            bucket.getValue().getPartitions().clear();
            for (int i = 0; i < bucketSize; i++) {
                topicPartition = new TopicPartition(config.topic(), ++partition);
                bucket.getValue().getPartitions().add(topicPartition);
                if (partition == partitions.size() - 1) {
                    break bucketAssign;
                }
            }
        }
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        // With the introduction of KIP-480 to enhance record production
        // throughput Kafka's API calls the partition() method twice resulting
        // in partitions being skipped. More information about this here:
        // https://issues.apache.org/jira/browse/KAFKA-9965
        // The temporary solution is to use the callback method 'onNewBatch'
        // to decrease the counter to stabilize the round-robin logic.
        String bucketName = lastBucket.get();
        Bucket bucket = buckets.get(bucketName);
        if (bucket != null) {
            bucket.decrementCounter();
        }
        lastBucket.remove();
    }

    @Override
    public void close() {
        // Nothing to close
    }
    
}
