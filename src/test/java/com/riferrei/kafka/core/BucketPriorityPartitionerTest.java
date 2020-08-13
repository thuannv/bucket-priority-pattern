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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BucketPriorityPartitionerTest {

    @Test
    public void checkMissingConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        try (BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner()) {
            // Check if the topic configuration is missing
            assertThrows(ConfigException.class, () -> {
                partitioner.configure(configs);
            });
            // Check if the buckets configuration is missing
            assertThrows(ConfigException.class, () -> {
                configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
                partitioner.configure(configs);
            });
            // Check if the allocation configuration is missing
            assertThrows(ConfigException.class, () -> {
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
                partitioner.configure(configs);
            });
            // Check if complete configuration is gonna be enough
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
                partitioner.configure(configs);
            });
        }
    }

    @Test
    public void checkMatchingBucketConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        try (BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner()) {
            assertThrows(InvalidConfigurationException.class, () -> {
                configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70");
                partitioner.configure(configs);
            });
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
                partitioner.configure(configs);
            });
        }
    }

    @Test
    public void checkAllocationPercentageConfiguration() {
        final Map<String, String> configs = new HashMap<>();
        try (BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner()) {
            assertThrows(InvalidConfigurationException.class, () -> {
                configs.put(BucketPriorityConfig.TOPIC_CONFIG, "test");
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 20");
                partitioner.configure(configs);
            });
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
                partitioner.configure(configs);
            });
        }
    }

    @Test
    public void checkMinNumberPartitions() {
        final String topic = "orders";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        // Using two buckets implies having at least two partitions...
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create a topic with only one partition...
        PartitionInfo partition0 = new PartitionInfo(topic, 0, null, null, null);
        List<PartitionInfo> partitions = List.of(partition0);
        Cluster cluster = new Cluster("test", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            assertThrows(InvalidConfigurationException.class, () -> {
                producer.send(new ProducerRecord<String, String>(topic, "Platinum-001", "value"));
            });
        }
    }

    @Test
    public void checkRoundRobinFallbackAction() {
        final String topic = "orders";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
        configs.put(BucketPriorityConfig.FALLBACK_ACTION_CONFIG, "RoundRobin");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create two partitions so the round robin algorithm can leverage
        PartitionInfo partition0 = new PartitionInfo(topic, 0, null, null, null);
        PartitionInfo partition1 = new PartitionInfo(topic, 1, null, null, null);
        List<PartitionInfo> partitions = List.of(partition0, partition1);
        Cluster cluster = new Cluster("test", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            List<Integer> chosenPartitions = new ArrayList<>();
            // Produce 10 different records
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record =
                    // Creating a record without key to force the fallback...
                    new ProducerRecord<String, String>(topic, null, "value");
                producer.send(record, (metadata, exception) -> {
                    chosenPartitions.add(metadata.partition());
                });
            }
            assertEquals(List.of(0,1,0,1,0,1,0,1,0,1), chosenPartitions);
        }
    }

    @Test
    public void checkDiscardFallbackAction() {
        final String topic = "orders";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70, 30");
        configs.put(BucketPriorityConfig.FALLBACK_ACTION_CONFIG, "Discard");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create two partitions to make things interesting but it doesn't matter
        PartitionInfo partition0 = new PartitionInfo(topic, 0, null, null, null);
        PartitionInfo partition1 = new PartitionInfo(topic, 1, null, null, null);
        List<PartitionInfo> partitions = List.of(partition0, partition1);
        Cluster cluster = new Cluster("test", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            int counter = 0;
            ProducerRecord<String, String> record = null;
            List<Integer> chosenPartitions = new ArrayList<>();
            // Produce 10 different records where 5 of them will
            // be sent using the key "Platinum" and thus need to
            // end up in the partition 0. The other 5 records will
            // be sent without a key and thus... will be discarded.
            for (int i = 0; i < 10; i++) {
                if (++counter > 5) {
                    record = new ProducerRecord<String, String>(topic, "Platinum", "value");
                } else {
                    record = new ProducerRecord<String, String>(topic, null, "value");
                }
                producer.send(record, (metadata, exception) -> {
                    // Ignoring partition -1 because this means
                    // not leveraging any available partition.
                    if (metadata.partition() != -1) {
                        chosenPartitions.add(metadata.partition());
                    }
                });
            }
            assertEquals(List.of(0, 0, 0, 0, 0), chosenPartitions);
        }
    }

}
