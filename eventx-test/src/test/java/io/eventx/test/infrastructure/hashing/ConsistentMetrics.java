package io.eventx.test.infrastructure.hashing;


import io.eventx.infrastructure.consistenthashing.Consistent;
import io.eventx.infrastructure.consistenthashing.member.Member;

import java.util.*;

import static java.lang.String.format;

public class ConsistentMetrics {

  private final Map<String, Set<Member>> keys = new HashMap<>();
  private final Map<Member, Integer> loads = new HashMap<>();

  private final Consistent target;

  private int missHits;

  public ConsistentMetrics(Consistent ring) {
    this.target = ring;
  }


  public void addMember(Member node) {
    loads.putIfAbsent(node, 0);
    target.addMember(node);
  }

  public void addAllMembers(Collection<Member> nodes) {
    nodes.forEach(n -> loads.putIfAbsent(n, 0));
    nodes.forEach(target::addMember);
  }


  public boolean contains(Member node) {
    return target.members().contains(node);
  }


  public void removeMember(Member node) {
    loads.remove(node);
    target.removeMember(node);
  }


  public Set<Member> members() {
    return new HashSet<>(target.members());
  }


  public Member locate(String key) {
    final var member = target.locate(key);
    int cnt = loads.getOrDefault(member, 0);
    loads.put(member, cnt + 1);
    Set<Member> prev = keys.computeIfAbsent(key, k -> new HashSet<>());
    if (! prev.isEmpty() && ! prev.contains(member)) {
      missHits++;
    }
    prev.add(member);
    return member;
  }

  public int size() {
    return target.size();
  }

  public void printLoadDistribution() {
    System.out.println();
    System.out.println("################### LOAD DISTRIBUTION ###################");
    System.out.println("Consistent hash: " + target.getClass().getName());
    System.out.println("Hasher: " + target.config().getHash64().getClass().getName());
    System.out.println("Replica count: " + target.config().getReplicaCount());
    System.out.println("Load factor: " + target.config().getLoadFactor());
    System.out.println("Size: " + target.size());
    System.out.println("____________________________________________________");
    for (Member node : loads.keySet()) {
      System.out.println("Node [" + node + "] received: " + loads.get(node) + " hits");
    }
    System.out.println("____________________________________________________");
    printExtrema();
    printStandardDeviation();
    System.out.println("____________________________________________________");
    System.out.println();
  }

  public void printMissHits(int reqCount) {
    System.out.println();
    System.out.println("########## MISS HITS ############");
    float percent = Math.round(missHits / (float) reqCount * 100);
    System.out.println("Nodes miss: [" + missHits + "] hits. - " + percent + "%");
    System.out.println();
  }

  public void printExtrema() {
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (Integer cnt : loads.values()) {
      if (cnt < min) min = cnt;
      if (cnt > max) max = cnt;
    }
    System.out.println(format("min: [%d] max: [%d], delta: [%d]", min, max, max - min));
    System.out.println("---");
  }

  public void printStandardDeviation() {
    double avg = calculateArithmeticMean();
    double dispersion = calculateDispersion(avg);
    double standDev = Math.round(Math.sqrt(dispersion));
    double standDevPercent = Math.round(standDev / avg * 100);
    System.out.println("arithmetic mean: [" + avg + "] hits");
    System.out.println("stan. deviation: [" + standDev + "] hits - " + standDevPercent + "%");
  }

  public double calculateDispersion(double avg) {
    double deviation = 0;
    for (Integer cnt : loads.values()) {
      deviation += Math.pow(avg - cnt, 2);
    }
    return Math.round(deviation / target.size());
  }

  private double calculateArithmeticMean() {
    float sum = 0;
    for (Integer cnt : loads.values()) {
      sum += cnt;
    }
    return Math.round(sum / target.size());
  }
}
