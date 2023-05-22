package io.eventx.test.infrastructure.hashing;

import io.eventx.infrastructure.models.AggregatePlainKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.eventx.infrastructure.consistenthashing.Consistent;
import io.eventx.infrastructure.consistenthashing.config.Config;
import io.eventx.infrastructure.consistenthashing.exceptions.EmptyHashRingException;
import io.eventx.infrastructure.consistenthashing.exceptions.MemberAlreadyAddedException;
import io.eventx.infrastructure.consistenthashing.exceptions.MemberNotFoundException;
import io.eventx.infrastructure.consistenthashing.member.Member;
import io.eventx.infrastructure.consistenthashing.member.impl.MemberImpl;
import io.eventx.infrastructure.models.AggregateKey;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashingTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsistentHashingTest.class);

  List<AggregatePlainKey> KEYS = IntStream.range(0, 1000000)
    .mapToObj(i -> new AggregatePlainKey("",UUID.randomUUID().toString(), "default"))
    .toList();

  private ArrayList<Member> createMembers(Consistent c, int numMembers) {
    ArrayList<Member> members = new ArrayList<>();
    for (int i = 1; i <= numMembers; i++) {
      Member m = new MemberImpl(String.format("node%d.olric.io", i));
      c.addMember(m);
      members.add(m);
    }

    return members;
  }

  @Test
  void simplePartitionedConsistentHashing() {
    //    Node [FIFTH_4e454162-b6cb-45c4-9a1b-89c9964ab731] received: 35765 hits
    //    Node [INITIAL::086e478a-5400-4685-914f-68d9f0c52958] received: 79822 hits
    final int CONSISTENCY_ITERATIONS = 1000;
    final int DISTRIBUTION_KEYS = 1000000;
    final int INITIAL_NODES = 10;
    int partitionRate = 100000;
    final var hashRing = HashRing.<SimpleNode>newBuilder()
      .name("proxyHashRing")
      .hasher(DefaultHasher.MURMUR_3)
      .partitionRate(40)
      .build();
    final var hashRingMetrics = new HashRingMetrics<>(hashRing);
    final var nodes = IntStream.range(0, INITIAL_NODES)
      .mapToObj(i -> SimpleNode.of("INITIAL::" + UUID.randomUUID().toString()))
      .toList();
    hashRing.addAll(nodes);
    final var atomicInt = new AtomicInteger(0);
    KEYS.forEach(key -> {
      atomicInt.addAndGet(1);
      removeNode(atomicInt, DISTRIBUTION_KEYS, 10, hashRingMetrics, nodes);
      addNode(atomicInt, DISTRIBUTION_KEYS, 8, hashRingMetrics, "FIRST_");
      addNode(atomicInt, DISTRIBUTION_KEYS, 7, hashRingMetrics, "SECOND_");
      removeNode(atomicInt, DISTRIBUTION_KEYS, 6, hashRingMetrics, nodes);
      removeNode(atomicInt, DISTRIBUTION_KEYS, 5, hashRingMetrics, nodes);
      addNode(atomicInt, DISTRIBUTION_KEYS, 4, hashRingMetrics, "THIRD_");
      addNode(atomicInt, DISTRIBUTION_KEYS, 4, hashRingMetrics, "FOURTH_");
      addNode(atomicInt, DISTRIBUTION_KEYS, 2, hashRingMetrics, "FIFTH_");
      removeNode(atomicInt, DISTRIBUTION_KEYS, 2, hashRingMetrics, nodes);
      hashRingMetrics.locate(key.aggregateId() + "::" + key.tenantId());
    });
    hashRingMetrics.printLoadDistribution();
    hashRingMetrics.printStandardDeviation();
    hashRingMetrics.printExtrema();
    final var key = new AggregatePlainKey("",UUID.randomUUID().toString(), "default");
    final var location = hashRingMetrics.locate(key.aggregateId() + "::" + key.tenantId()).get();
    IntStream.range(0, CONSISTENCY_ITERATIONS).forEach(i -> {
      final var loc = hashRingMetrics.locate(key.aggregateId() + "::" + key.tenantId());
      assertEquals(location.getKey(), loc.get().getKey(), "Hashing inconsistent at iteration number -> " + i);
    });
    final var nodeToRemove = nodes.stream().findFirst().get();
    hashRingMetrics.remove(nodeToRemove);
    IntStream.range(0, CONSISTENCY_ITERATIONS).forEach(i -> {
      final var loc = hashRingMetrics.locate(key.aggregateId() + "::" + key.tenantId());
      assertEquals(location.getKey(), loc.get().getKey(), "Hashing inconsistent at iteration number -> " + i);
    });
    hashRingMetrics.add(nodeToRemove);
    IntStream.range(0, CONSISTENCY_ITERATIONS).forEach(i -> {
      final var loc = hashRingMetrics.locate(key.aggregateId() + "::" + key.tenantId());
      assertEquals(location.getKey(), loc.get().getKey(), "Hashing inconsistent at iteration number -> " + i);
    });
  }

  private static void addNode(final AtomicInteger atomicInt, final int DISTRIBUTION_KEYS, final int x, final HashRingMetrics<SimpleNode> hashRingMetrics, final String SECOND_) {
    if (atomicInt.get() == DISTRIBUTION_KEYS / x) {
      hashRingMetrics.add(SimpleNode.of(SECOND_ + UUID.randomUUID().toString()));
    }
  }

  private static void removeNode(final AtomicInteger atomicInt, final int DISTRIBUTION_KEYS, final int x, final HashRingMetrics<SimpleNode> hashRingMetrics, final List<SimpleNode> nodes) {
    if (atomicInt.get() == DISTRIBUTION_KEYS / x) {
      hashRingMetrics.remove(nodes.stream().findFirst().filter(n -> n.getKey().contains("INITIAL")).get());
    }
  }

  @Test
  void boundedLoadConsistentHashing() {
    // loadFactor -> 0.5
    // replicaCount -> 40
    // Node [MemberImpl[name=FIFTH::783c20dc-40f8-4d96-a26c-901b18153a25]] received: 60586 hits
    // Node [MemberImpl[name=INITIAL::916efa76-4a6c-4c41-a4d5-9ce03741fc44]] received: 102595 hits

    //
    final var config = new Config();
    config.setReplicaCount(50);
    config.setLoadFactor(0.1);
    final var hashRing = new ConsistentMetrics(new Consistent(config));
    final var members = IntStream.range(0, 10)
      .mapToObj(i -> new MemberImpl("INITIAL::" +UUID.randomUUID().toString()))
      .toList();
    members.forEach(hashRing::addMember);
    final var atomicInt = new AtomicInteger(0);
    KEYS.forEach(key -> {
      atomicInt.addAndGet(1);
      addMember(atomicInt, 2, hashRing, "FIFTH::");
      removeMember(atomicInt, 2, hashRing);
      addMember(atomicInt, 4, hashRing, "FOURTH::");
      addMember(atomicInt, 4, hashRing, "THIRD::");
      removeMember(atomicInt, 6, hashRing);
      removeMember(atomicInt, 5, hashRing);
      addMember(atomicInt, 8, hashRing, "SECOND::");
      addMember(atomicInt, 7, hashRing, "FIRST::");
      removeMember(atomicInt, 10, hashRing);
//      final var start = Instant.now();
      hashRing.locate(key.aggregateId() + "::" + key.tenantId());
//      LOGGER.info("Located in " + Duration.between(start, Instant.now()).toMillis() + "ms");
    });
    hashRing.printLoadDistribution();
    hashRing.printStandardDeviation();
    hashRing.printExtrema();
//    final var key = keys.stream().findFirst().get();
//    final var location = hashRing.locate(key.aggregateId() + "::" + key.tenantId());
//    iterate(hashRing, key, location);
//    final var nodeToRemove = hashRing.members().stream().findFirst().get();
//    hashRing.removeMember(nodeToRemove);
//    iterate(hashRing, key, location);
//    hashRing.addMember(nodeToRemove);
//    hashRing.addMember(new MemberImpl(UUID.randomUUID().toString()));
//    iterate(hashRing, key, location);
  }

  private void removeMember(final AtomicInteger atomicInt, final int x, final ConsistentMetrics hashRing) {
    if (atomicInt.get() == KEYS.size() / x) {
      hashRing.removeMember(hashRing.members().stream().filter(m -> m.name().contains("INITIAL")).findFirst().get());
    }
  }

  private void addMember(final AtomicInteger atomicInt, final int x, final ConsistentMetrics hashRing, final String x1) {
    if (atomicInt.get() == KEYS.size() / x) {
      hashRing.addMember(new MemberImpl(x1 + UUID.randomUUID().toString()));
    }
  }

  private static void iterate(final ConsistentMetrics hashRing, final AggregateKey key, final Member location) {
    IntStream.range(0, 1000).forEach(i -> {
      final var loc = hashRing.locate(key.aggregateId() + "::" + key.tenantId());
      assertEquals(location.name(), loc.name(), "Hashing inconsistent at iteration number -> " + i);
    });
  }

  @Test
  public void createEmptyConsistentInstanceWithDefaultConfig() {
    Consistent c = new Consistent(Config.getConfig());
    assertThrows(EmptyHashRingException.class, () -> c.locate("foobar"));
  }

  @Test
  public void locateOneWithOneMember() {
    Consistent c = new Consistent(Config.getConfig());

    Member m = new MemberImpl("node1.olric.io");
    c.addMember(m);

    Member located = c.locate("foobar");
    assertEquals(m, located);
  }

  public void memberAlreadyAddedException() {
    Consistent c = new Consistent(Config.getConfig());

    Member m = new MemberImpl("node1.olric.io");
    c.addMember(m);
    assertThrows(MemberAlreadyAddedException.class, () -> c.addMember(m));

  }

  @Test
  public void locateManyWithOneMember() {
    Consistent c = new Consistent(Config.getConfig());

    Member m = new MemberImpl("node1.olric.io");
    c.addMember(m);

    for (int i = 0; i < 100; i++) {
      Member located = c.locate(String.format("foobar-%d", i));
      assertEquals(m, located);
    }
  }

  @Test
  public void incrAndGetLoadWithOneMember() {
    Consistent c = new Consistent(Config.getConfig());

    Member m = new MemberImpl("node1.olric.io");
    c.addMember(m);

    for (int i = 0; i < 100; i++) {
      Member located = c.locate(String.format("foobar-%d", i));
      c.incrLoad(located);
    }

    assertEquals((Integer) 100, c.getLoad(m));
    assertTrue(c.getLoad(m) < c.averageLoad());
  }

  @Test
  public void incrAndGetLoadWithManyMember() {
    Consistent c = new Consistent(Config.getConfig());

    Member m1 = new MemberImpl("node1.olric.io");
    c.addMember(m1);

    Member m2 = new MemberImpl("node2.olric.io");
    c.addMember(m2);

    Member m3 = new MemberImpl("node3.olric.io");
    c.addMember(m3);

    for (int i = 0; i < 100; i++) {
      Member located = c.locate(String.format("foobar-%d", i));
      c.incrLoad(located);
    }

    assertTrue(c.getLoad(m1) < 100);
    assertTrue(c.getLoad(m1) < c.averageLoad());
  }

  @Test
  public void incrDecrAndGetLoadWithManyMember() {
    Consistent c = new Consistent(Config.getConfig());
    int numMembers = 3;
    ArrayList<Member> members = createMembers(c, numMembers);

    for (int i = 0; i < 100; i++) {
      Member located = c.locate(String.format("foobar-%d", i));
      c.incrLoad(located);

      // do something with the selected node
      c.decrLoad(located);
    }

    for (int i = 1; i <= numMembers; i++) {
      Member m = members.get(i - 1);
      assertEquals((Integer) 0, c.getLoad(m));
      assertTrue(c.getLoad(m) < c.averageLoad());
    }
  }

  @Test
  public void getMembers() {
    Consistent c = new Consistent(Config.getConfig());
    int numMembers = 3;
    ArrayList<Member> members = createMembers(c, numMembers);

    assertEquals(members, c.members());
  }

  @Test
  public void checkSize() {
    Consistent c = new Consistent(Config.getConfig());
    int numMembers = 3;
    createMembers(c, numMembers);
    assertEquals(numMembers, c.size());
  }

  @Test
  public void removeMemberAndCheckSize() {
    Consistent c = new Consistent(Config.getConfig());
    int numMembers = 3;
    ArrayList<Member> members = createMembers(c, numMembers);

    for (Member member : members) {
      c.removeMember(member);
    }

    assertEquals(0, c.size());
  }

  @Test
  public void removeMemberAndAverageLoad() {
    Consistent c = new Consistent(Config.getConfig());
    int numMembers = 3;
    ArrayList<Member> members = createMembers(c, numMembers);

    for (Member member : members) {
      c.removeMember(member);
    }

    assertEquals(0, c.averageLoad(), 0.0);
  }

  @Test
  public void incrLoadMemberNotFound() {
    Consistent c = new Consistent(Config.getConfig());
    Member m = new MemberImpl("node1.olric.io");
    assertThrows(MemberNotFoundException.class, () -> c.incrLoad(m));
  }

  @Test
  public void decrLoadMemberNotFound() {
    Consistent c = new Consistent(Config.getConfig());
    Member m = new MemberImpl("node1.olric.io");
    assertThrows(MemberNotFoundException.class, () -> c.decrLoad(m));
  }

  @Test
  public void minimumLoad() {
    Consistent c = new Consistent(Config.getConfig());
    Member m = new MemberImpl("node1.olric.io");
    c.addMember(m);
    c.decrLoad(m);
    Integer load = c.getLoad(m);
    assertEquals((Integer) 0, load);
  }

}
