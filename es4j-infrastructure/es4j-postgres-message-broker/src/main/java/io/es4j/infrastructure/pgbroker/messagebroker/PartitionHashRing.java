package io.es4j.infrastructure.pgbroker.messagebroker;

import io.es4j.infrastructure.pgbroker.exceptions.PartitionNotFound;
import io.es4j.infrastructure.pgbroker.models.MessagePartition;
import io.es4j.infrastructure.pgbroker.models.PartitionKey;
import io.es4j.infrastructure.pgbroker.models.PartitionQuery;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.smallrye.mutiny.Uni;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;


public class PartitionHashRing {

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionHashRing.class);
    private static final HashRing<SimpleNode> PARTITION_HASH_RING = HashRing.<SimpleNode>newBuilder()
        .name("queue-partitions-hash-ring")       // set hash ring fileName
        .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
        .partitionRate(100000)                  // number of partitions per node
        .build();

    // todo put a subscription on the partitionId store so that we can update the hash ring  dynamically when partitions are added or removed
    public static Uni<Void> populateHashRing(Repository<PartitionKey, MessagePartition, PartitionQuery> partitionRepository) {
        if (INITIALIZED.compareAndSet(false, true)) {
            return partitionRepository.query("select * from queue_partition")
                .flatMap(partitions -> partitionRepository.repositoryHandler().vertx().executeBlocking(
                    Uni.createFrom().item(
                        () -> {
                            partitions.forEach(partition -> PARTITION_HASH_RING.add(SimpleNode.of(partition.partitionId())));
                            return Void.TYPE;
                        }
                    )
                ))
                .onFailure(NotFound.class).recoverWithNull().replaceWithVoid();
        }
        return Uni.createFrom().voidItem();
    }

    public static String resolve(String partitionKey) {
        LOGGER.debug("Locating partition for key {}", partitionKey);
        if (Objects.nonNull(partitionKey) && PARTITION_HASH_RING.size() > 0) {
            return PARTITION_HASH_RING.locate(partitionKey).orElseThrow(PartitionNotFound::new).getKey();
        }
        return "none";
    }


}
