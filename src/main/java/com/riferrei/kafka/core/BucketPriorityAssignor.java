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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;

public class BucketPriorityAssignor extends CooperativeStickyAssignor implements Configurable {

    private BucketPriorityConfig config;
    private Map<String, Bucket> buckets;
    private int lastPartitionCount;

    @Override
    public String name() {
        return "bucket-priority";
    }

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
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        ByteBuffer userData = null;
        for (String topic : topics) {
            if (topic.equals(config.topic())) {
                String bucket = config.bucket();
                Charset charset = StandardCharsets.UTF_8;
                userData = charset.encode(bucket);
                break;
            }
        }
        return userData;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
        Map<String, Subscription> subscriptions) {
        int numPartitions = partitionsPerTopic.get(config.topic());
        // Check if the # of partitions has changed
        // and trigger an update if that happened.
        if (lastPartitionCount != numPartitions) {
            updatePartitionsAssignment(numPartitions);
            lastPartitionCount = numPartitions;
        }
        // Create a first version of the assignments
        // using the strategy inherited from super.
        Map<String, List<TopicPartition>> assignments = super.assign(partitionsPerTopic, subscriptions);
        // Remove from the current assignments all the
        // consumers that are using a bucket priority
        // strategy. The remaining assignments should
        // be used as they are.
        List<String> consumersToRemove = new ArrayList<>();
        Map<String, List<TopicPartition>> bucketAssignments = new LinkedHashMap<>();
        Map<String, List<String>> consumersPerBucket = new LinkedHashMap<>();
        for (String consumer : assignments.keySet()) {
            Subscription subscription = subscriptions.get(consumer);
            if (subscription.topics().contains(config.topic())) {
                bucketAssignments.put(consumer, new ArrayList<>());
                ByteBuffer userData = subscription.userData();
                Charset charset = StandardCharsets.UTF_8;
                String bucket = charset.decode(userData).toString();
                if (buckets.containsKey(bucket)) {
                    if (consumersPerBucket.containsKey(bucket)) {
                        List<String> consumers = consumersPerBucket.get(bucket);
                        consumers.add(consumer);
                    } else {
                        List<String> consumers = new ArrayList<>();
                        consumers.add(consumer);
                        consumersPerBucket.put(bucket, consumers);
                    }
                }
                consumersToRemove.add(consumer);
            }
        }
        // Remove the consumers that should use bucket priority
        consumersToRemove.stream().forEach(c -> assignments.remove(c));
        // Evenly distribute the partitions across the
        // available consumers in a per-bucket basis.
        AtomicInteger counter = new AtomicInteger(-1);
        for (Map.Entry<String, Bucket> bucket : buckets.entrySet()) {
            List<String> consumers = consumersPerBucket.get(bucket.getKey());
            // Check if the bucket has consumers available...
            if (consumers != null && !consumers.isEmpty()) {
                for (TopicPartition partition : bucket.getValue().getPartitions()) {
                    int nextValue = counter.incrementAndGet();
                    int index = Utils.toPositive(nextValue) % consumers.size();
                    String consumer = consumers.get(index);
                    bucketAssignments.get(consumer).add(partition);
                }
            }
        }
        // Finally merge the two assignments back
        assignments.putAll(bucketAssignments);
        return assignments;
    }

    private void updatePartitionsAssignment(int numPartitions) {
        List<TopicPartition> partitions = partitions(config.topic(), numPartitions);
        if (partitions.size() < buckets.size()) {
            StringBuilder message = new StringBuilder();
            message.append("The number of partitions available for the topic '");
            message.append(config.topic()).append("' is incompatible with the ");
            message.append("number of buckets. It needs to be at least ");
            message.append(buckets.size()).append(".");
            throw new InvalidConfigurationException(message.toString());
        }
        // Sort partitions in ascendent order since
        // the partitions will be mapped into the
        // buckets from partition-0 to partition-n.
        partitions = partitions.stream()
            .sorted(Comparator.comparing(TopicPartition::partition))
            .collect(Collectors.toList());
        // Design the layout of the distribution
        int distribution = 0;
        Map<String, Integer> layout = new LinkedHashMap<>();
        for (Map.Entry<String, Bucket> entry : buckets.entrySet()) {
            int bucketSize = entry.getValue().size(partitions.size());
            layout.put(entry.getKey(), bucketSize);
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
        TopicPartition topicPartition = null;
        bucketAssign: for (Map.Entry<String, Bucket> entry : buckets.entrySet()) {
            int bucketSize = layout.get(entry.getKey());
            entry.getValue().getPartitions().clear();
            for (int i = 0; i < bucketSize; i++) {
                topicPartition = new TopicPartition(config.topic(), ++partition);
                entry.getValue().getPartitions().add(topicPartition);
                if (partition == partitions.size() - 1) {
                    break bucketAssign;
                }
            }
        }
    }
    
}
