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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

public class Bucket implements Comparable<Bucket> {

    private int allocation;
    private List<TopicPartition> partitions;
    private AtomicInteger counter;

    public Bucket(int allocation) {
        this.allocation = allocation;
        partitions = new ArrayList<>();
        counter = new AtomicInteger(-1);
    }

    public int nextPartition() {
        if (!partitions.isEmpty()) {
            int nextValue = counter.incrementAndGet();
            int index = Utils.toPositive(nextValue) % partitions.size();
            return partitions.get(index).partition();
        }
        return -1;
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

    public void decrementCounter() {
        counter.decrementAndGet();
    }

    public int size(int numPartitions) {
        return Math.round(((float) allocation / 100) * numPartitions);
    }

    public int getAllocation() {
        return allocation;
    }

    public List<TopicPartition> getPartitions() {
        return partitions;
    }

}
