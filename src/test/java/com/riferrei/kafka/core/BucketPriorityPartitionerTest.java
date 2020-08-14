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
import java.util.concurrent.atomic.AtomicInteger;

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
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
                partitioner.configure(configs);
            });
            // Check if complete configuration is gonna be enough
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
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
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%");
                partitioner.configure(configs);
            });
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
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
                configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 20%");
                partitioner.configure(configs);
            });
            assertDoesNotThrow(() -> {
                configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
                partitioner.configure(configs);
            });
        }
    }

    @Test
    public void checkMinNumberPartitions() {
        final String topic = "test";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        // Using two buckets implies having at least two partitions...
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
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
                producer.send(new ProducerRecord<String, String>(topic, "B1-001", "value"));
            });
        }
    }

    @Test
    public void checkRoundRobinFallbackAction() {
        final String topic = "test";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
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
            ProducerRecord<String, String> record = null;
            List<Integer> chosenPartitions = new ArrayList<>();
            // Produce a record with a wrong topic to force fallback...
            record = new ProducerRecord<String, String>("customers", "B1", "value");
            producer.send(record, (metadata, exception) -> {
                if (metadata.topic().equals(topic)) {
                    // This is not supposed to happen anyway...
                    chosenPartitions.add(metadata.partition());
                }
            });
            // Produce a record without a key to force fallback...
            record = new ProducerRecord<String, String>(topic, null, "value");
            producer.send(record, (metadata, exception) -> {
                chosenPartitions.add(metadata.partition());
            });
            // Using a key without a valid bucket to force fallback...
            record = new ProducerRecord<String, String>(topic, "Wrong", "value");
            producer.send(record, (metadata, exception) -> {
                chosenPartitions.add(metadata.partition());
            });
            // Now produce three records with valid topic and key...
            record = new ProducerRecord<String, String>(topic, "B1-001", "value");
            producer.send(record, (metadata, exception) -> {
                chosenPartitions.add(metadata.partition());
            });
            record = new ProducerRecord<String, String>(topic, "B1-002", "value");
            producer.send(record, (metadata, exception) -> {
                chosenPartitions.add(metadata.partition());
            });
            record = new ProducerRecord<String, String>(topic, "B2-001", "value");
            producer.send(record, (metadata, exception) -> {
                chosenPartitions.add(metadata.partition());
            });
            // The expected output is:
            // - First two records are spread over the 2 partitions
            // - Two records need to be sent to partition 0 (B1)
            // - One record need to be sent to partition 1 (B2)
            assertEquals(List.of(0, 1, 0, 0, 1), chosenPartitions);
        }
    }

    @Test
    public void checkDiscardFallbackAction() {
        final String topic = "test";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
        configs.put(BucketPriorityConfig.FALLBACK_ACTION_CONFIG, "Discard");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create two partitions to make things a little bit more interesting
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
            // be sent using the key "B1" and thus need to end up
            // in the partition 0. The other 5 records will be sent
            // without a key and thus... will be discarded.
            for (int i = 0; i < 10; i++) {
                if (++counter > 5) {
                    record = new ProducerRecord<String, String>(topic, "B1", "value");
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
            // The expected output is:
            // - The first 5 records need to be discarded
            // - The last 5 records need to end up on partition 0 (B1)
            assertEquals(List.of(0, 0, 0, 0, 0), chosenPartitions);
        }
    }

    @Test
    public void checkEvenBucketAllocation() {
        final String topic = "test";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2, B3");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "50%, 30%, 20%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create 10 partitions for buckets B1, B2, and B3
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = new Cluster("test", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            ProducerRecord<String, String> record = null;
            // Produce 10 records to the 'B1' bucket that must
            // be composed of the partitions [0, 1, 2, 3, 4]
            final AtomicInteger b1Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 0 && chosenPartition <= 4) {
                        b1Count.incrementAndGet();
                    }
                });
            }
            // Produce 10 records to the 'B2' bucket that must
            // be composed of the partitions [5, 6, 7]
            final AtomicInteger b2Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 5 && chosenPartition <= 7) {
                        b2Count.incrementAndGet();
                    }
                });
            }
            // Produce 10 records to the 'B3' bucket that must
            // be composed of the partitions [8, 9]
            final AtomicInteger b3Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B3-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 8 && chosenPartition <= 9) {
                        b3Count.incrementAndGet();
                    }
               });
            }
            // The expected output is:
            // - B1 should contain 10 records
            // - B2 should contain 10 records
            // - B3 should contain 10 records
            assertEquals(10, b1Count.get());
            assertEquals(10, b2Count.get());
            assertEquals(10, b3Count.get());
        }
    }

    @Test
    public void checkUnevenBucketAllocation() {
        final String topic = "test";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2, B3");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "55%, 40%, 5%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create 10 partitions for buckets B1, B2, and B3
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = new Cluster("test", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            ProducerRecord<String, String> record = null;
            // Produce 10 records to the 'B1' bucket that must
            // be composed of the partitions [0, 1, 2, 3, 4, 5]
            final AtomicInteger b1Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 0 && chosenPartition <= 5) {
                        b1Count.incrementAndGet();
                    }
                });
            }
            // Produce 10 records to the 'B2' bucket that must
            // be composed of the partitions [6, 7, 8, 9]
            final AtomicInteger b2Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 6 && chosenPartition <= 9) {
                        b2Count.incrementAndGet();
                    }
                });
            }
            // Produce 10 records to the 'B3' bucket that must
            // be composed of zero partitions [] because is uneven
            final AtomicInteger b3Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B3-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    // Getting a partition set to -1 means that
                    // the record didn't get written anywhere and
                    // therefore we can expect that the counter
                    // will never be incremented below.
                    if (metadata.partition() != -1) {
                        b3Count.incrementAndGet();
                    }
                });
            }
            // The expected output is:
            // - B1 should contain 10 records
            // - B2 should contain 10 records
            // - B3 should contain 0 records
            assertEquals(10, b1Count.get());
            assertEquals(10, b2Count.get());
            assertEquals(0, b3Count.get());
        }
    }

    @Test
    public void checkUnevenPartitionAllocation() {
        final String topic = "test";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2, B3");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "55%, 40%, 5%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create 5 partitions for buckets B1, B2, and B3
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = new Cluster("test", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            ProducerRecord<String, String> record = null;
            // Produce 10 records to the 'B1' bucket that must
            // be composed of the partitions [0, 1, 2]
            final AtomicInteger b1Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 0 && chosenPartition <= 2) {
                        b1Count.incrementAndGet();
                    }
                });
            }
            // Produce 10 records to the 'B2' bucket that must
            // be composed of the partitions [3, 4]
            final AtomicInteger b2Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    if (chosenPartition >= 3 && chosenPartition <= 4) {
                        b2Count.incrementAndGet();
                    }
                });
            }
            // Produce 10 records to the 'B3' bucket that must
            // be composed of zero partitions [] because is uneven
            final AtomicInteger b3Count = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                String recordKey = "B3-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    // Getting a partition set to -1 means that
                    // the record didn't get written anywhere and
                    // therefore we can expect that the counter
                    // will never be incremented below.
                    if (metadata.partition() != -1) {
                        b3Count.incrementAndGet();
                    }
                });
            }
            // The expected output is:
            // - B1 should contain 10 records
            // - B2 should contain 10 records
            // - B3 should contain 0 records
            assertEquals(10, b1Count.get());
            assertEquals(10, b2Count.get());
            assertEquals(0, b3Count.get());
        }
    }

    @Test
    public void checkBucketDataDistribution() {
        final String topic = "test";
        Map<String, String> configs = new HashMap<>();
        configs.put(BucketPriorityConfig.TOPIC_CONFIG, topic);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "B1, B2");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "80%, 20%");
        BucketPriorityPartitioner partitioner = new BucketPriorityPartitioner();
        partitioner.configure(configs);
        // Create 10 partitions for buckets B1 and B2 that
        // will create the following partition assignment:
        // B1 = [0, 1, 2, 3, 4, 5, 6, 7]
        // B2 = [8, 9]
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(new PartitionInfo(topic, i, null, null, null));
        }
        Cluster cluster = new Cluster("test", new ArrayList<Node>(),
            partitions, Set.of(), Set.of());
        try (MockProducer<String, String> producer = new MockProducer<>(cluster,
            true, partitioner, new StringSerializer(), new StringSerializer())) {
            final Map<Integer, Integer> distribution = new HashMap<>();
            ProducerRecord<String, String> record = null;
            // Produce 16 records to the 'B1' bucket that
            // should distribute 2 records per partition.
            for (int i = 0; i < 16; i++) {
                String recordKey = "B1-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }
            // Produce 16 records to the 'B2' bucket that
            // should distribute 8 records per partition.
            for (int i = 0; i < 16; i++) {
                String recordKey = "B2-" + i;
                record = new ProducerRecord<String, String>(topic, recordKey, "value");
                producer.send(record, (metadata, exception) -> {
                    int chosenPartition = metadata.partition();
                    int currentCount = 0;
                    if (distribution.containsKey(chosenPartition)) {
                        currentCount = distribution.get(chosenPartition);
                    }
                    distribution.put(chosenPartition, ++currentCount);
                });
            }
            // The expected output is:
            // - 2 records on each partition of B1
            // - 8 records on each partition of B2
            Map<Integer, Integer> expected = Map.of(
                0, 2,
                1, 2,
                2, 2,
                3, 2,
                4, 2,
                5, 2,
                6, 2,
                7, 2,
                8, 8,
                9, 8
            );
            assertEquals(expected, distribution);
        }
    }

}
