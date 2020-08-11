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
import java.util.ArrayList;
import java.util.Arrays;
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

import static com.riferrei.kafka.core.Bucket.size;

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
                Charset charset = Charset.forName("UTF-8");
                userData = charset.encode(bucket);
                break;
            }
        }
        return userData;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
        Map<String, Subscription> subscriptions) {
        int partitionCount = partitionsPerTopic.get(config.topic());
        // Check if the # of partitions has changed
        // and trigger an update if that happened.
        if (lastPartitionCount != partitionCount) {
            updateBucketPartitions(partitionCount);
            lastPartitionCount = partitionCount;
        }
        // Create a first version of the assignments
        // using the strategy inherited from super.
        Map<String, List<TopicPartition>> assignments = super.assign(partitionsPerTopic, subscriptions);
        // Remove from the current assignments all the
        // consumers that are using a bucket priority
        // strategy. The remaining assignments should
        // be used as they are.
        Map<String, List<TopicPartition>> customAssignments = new LinkedHashMap<>();
        Map<String, List<String>> consumersPerBucket = new LinkedHashMap<>();
        for (String consumer : assignments.keySet()) {
            Subscription subscription = subscriptions.get(consumer);
            if (subscription.topics().contains(config.topic())) {
                ByteBuffer userData = subscription.userData();
                Charset charset = Charset.forName("UTF-8");
                String bucket = charset.decode(userData).toString();
                if (buckets.containsKey(bucket)) {
                    customAssignments.put(consumer, assignments.get(consumer));
                    assignments.remove(consumer);
                    if (consumersPerBucket.containsKey(bucket)) {
                        List<String> consumers = consumersPerBucket.get(bucket);
                        consumers.add(consumer);
                    } else {
                        consumersPerBucket.put(bucket, Arrays.asList(consumer));
                    }
                }
            }
        }
        // Clear whatever assignments has been made
        // by super. The new assignments need to be
        // based on the allocation of each bucket.
        customAssignments.entrySet().stream()
            .forEach(entry -> entry.getValue().clear());
        // Evenly distribute the partitions across the
        // available consumers in a per-bucket basis.
        AtomicInteger counter = new AtomicInteger(-1);
        for (String bucketName : buckets.keySet()) {
            Bucket bucket = buckets.get(bucketName);
            List<String> consumers = consumersPerBucket.get(bucketName);
            // Check if the bucket has consumers available...
            if (consumers != null && !consumers.isEmpty()) {
                for (TopicPartition partition : bucket.getPartitions()) {
                    int nextValue = counter.incrementAndGet();
                    int index = Utils.toPositive(nextValue) % consumers.size();
                    String consumer = consumers.get(index);
                    customAssignments.get(consumer).add(partition);
                }
            }
        }
        // Finally merge the two assignments back
        assignments.putAll(customAssignments);
        return assignments;
    }

    private void updateBucketPartitions(int partitionCount) {
        List<TopicPartition> partitions = super.partitions(config.topic(), partitionCount);
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
            .sorted(Comparator.comparing(TopicPartition::partition))
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
        TopicPartition topicPartition = null;
        bucketAssign: for (String bucketName : buckets.keySet()) {
            Bucket bucket = buckets.get(bucketName);
            int bucketSize = layout.get(bucketName);
            bucket.getPartitions().clear();
            for (int i = 0; i < bucketSize; i++) {
                topicPartition = new TopicPartition(config.topic(), ++partition);
                bucket.getPartitions().add(topicPartition);
                if (partition == partitions.size() - 1) {
                    break bucketAssign;
                }
            }
        }
    }
    
}
