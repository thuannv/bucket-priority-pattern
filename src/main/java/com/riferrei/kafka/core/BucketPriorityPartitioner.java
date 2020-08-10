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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketPriorityPartitioner implements Partitioner {

    private final Logger log = LoggerFactory.getLogger(BucketPriorityPartitioner.class);

    private Partitioner fallbackPartitioner;
    private BucketPriorityPartitionerConfig config;
    private ThreadLocal<String> lastBucket;
    private Map<String, Bucket> buckets;
    private int lastPartitionCount;

    @Override
    public void configure(Map<String, ?> configs) {
        config = new BucketPriorityPartitionerConfig(configs);
        List<Integer> bucketAlloc = new ArrayList<>(config.allocation().size());
        for (String bucketAllocItem : config.allocation()) {
            bucketAlloc.add(Integer.parseInt(bucketAllocItem.trim()));
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
            buckets.put(bucketName, new Bucket(bucketName, bucketAlloc.get(i)));
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
            if (key != null && key instanceof String) {
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
        for (String bucketName : buckets.keySet()) {
            Bucket bucket = buckets.get(bucketName);
            int bucketSize = size(bucket.getAllocation(), partitions.size());
            layout.put(bucketName, bucketSize);
            distribution += bucketSize;
        }
        // Check if there are unassigned partitions.
        // If so then distribute them over the buckets
        // starting from the top to bottom until there
        // are no partitions left.
        int remaining = partitions.size() - distribution;
        Iterator<String> iter = buckets.keySet().iterator();
        while (remaining > 0) {
            String bucketName = iter.next();
            int bucketSize = layout.get(bucketName);
            layout.put(bucketName, ++bucketSize);
            remaining--;
        }
        // Finally assign the available partitions to buckets
        int partition = -1;
        bucketAssign: for (String bucketName : buckets.keySet()) {
            Bucket bucket = buckets.get(bucketName);
            int bucketSize = layout.get(bucketName);
            bucket.clearExistingPartitions();
            for (int i = 0; i < bucketSize; i++) {
                bucket.addPartition(++partition);
                if (partition == partitions.size() - 1) {
                    break bucketAssign;
                }
            }
        }
    }

    private int size(int allocation, int partitionCount) {
        return Math.round(((float) allocation / 100) * partitionCount);
    }

    private class Bucket implements Comparable<Bucket> {

        private String bucketName;
        private int allocation;
        private List<TopicPartition> partitions;
        private AtomicInteger counter;

        public Bucket(String bucketName, int allocation) {
            this.bucketName = bucketName;
            this.allocation = allocation;
            partitions = new ArrayList<>();
            counter = new AtomicInteger(-1);
        }

        public int nextPartition() {
            int nextPartition = -1;
            if (!partitions.isEmpty()) {
                int nextValue = counter.incrementAndGet();
                int index = Utils.toPositive(nextValue) % partitions.size();
                nextPartition = partitions.get(index).partition();
            } else {
                StringBuilder message = new StringBuilder();
                message.append("The bucket '%s' doesn't have any partitions ");
                message.append("assigned. This means that any record meant for ");
                message.append("this bucket will be lost. Please adjust the ");
                message.append("allocation configuration and/or increase the ");
                message.append("number of partitions for the topic '%s' to ");
                message.append("avoid losing records.");
                log.warn(message.toString(), getBucketName(), config.topic());
            }
            return nextPartition;
        }

        public void clearExistingPartitions() {
            getPartitions().clear();
        }

        public void addPartition(int partition) {
            getPartitions().add(new TopicPartition(
                config.topic(), partition));
        }

        public void decrementCounter() {
            counter.decrementAndGet();
        }

        @Override
        public int compareTo(Bucket bucket) {
            int result = 0;
            if (getAllocation() < bucket.getAllocation()) {
                result = 1;
            } else if (getAllocation() > bucket.getAllocation()) {
                result = -1;
            }
            return result;
        }

        public String getBucketName() {
            return bucketName;
        }

        public int getAllocation() {
            return allocation;
        }

        public List<TopicPartition> getPartitions() {
            return partitions;
        }

    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        // With the introduction of KIP-480 to enhance record production
        // throughput Kafka's API calls the partition() method twice resulting
        // in partitions being skipped. More information about this here:
        // https://issues.apache.org/jira/browse/KAFKA-9965
        // The temporary solution is to use the callback method 'onNewBatch'
        // to decrease the counter to stabilized the round-robin logic.
        String bucketName = lastBucket.get();
        Bucket bucket = buckets.get(bucketName);
        if (bucket != null) {
            bucket.decrementCounter();
        }
        lastBucket.remove();
    }

    @Override
    public void close() {
    }
    
}
