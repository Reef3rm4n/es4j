/*
 * Copyright (c) 2021 Burak Sezer
 * All rights reserved.
 *
 * This code is licensed under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files(the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions :
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package io.eventx.infrastructure.consistenthashing;

import io.eventx.infrastructure.consistenthashing.config.Config;
import io.eventx.infrastructure.consistenthashing.exceptions.EmptyHashRingException;
import io.eventx.infrastructure.consistenthashing.exceptions.MemberAlreadyAddedException;
import io.eventx.infrastructure.consistenthashing.exceptions.MemberNotFoundException;
import io.eventx.infrastructure.consistenthashing.member.Member;

import java.util.*;

public class Consistent {
    private final Config config;
    private final SortedMap<Long, Member> hashRing;
    private final HashSet<String> members;
    private final HashMap<Member, Integer> loads;
    private int totalLoad;

    public Consistent(Config c) {
        this.config = c;
        this.members = new HashSet<>();
        this.loads = new HashMap<>();
        this.hashRing = new TreeMap<>();
    }

    public void addMember(Member m) throws MemberAlreadyAddedException {
        if (members.contains(m.name())) {
            throw new MemberAlreadyAddedException();
        }

        for (int i = 0; i < config.getReplicaCount(); i++) {
            String replica = String.format("%s-%d", m.name(), i);
            long rkey = config.getHash64().hash64(replica);
            hashRing.put(rkey, m);
        }

        members.add(m.name());
        loads.put(m, 0);
    }

    public void removeMember(Member m) {
        if (!members.contains(m.name())) {
            return;
        }

        for (int i = 0; i < config.getReplicaCount(); i++) {
            String replica = String.format("%s-%d", m.name(), i);
            long rkey = config.getHash64().hash64(replica);
            hashRing.remove(rkey, m);
        }

        long load = loads.get(m);
        totalLoad -= load;
        loads.remove(m);
        members.remove(m.name());
    }

    public double averageLoad() {
        int numMembers = members.size();

        if (numMembers == 0) {
            return 0;
        }

        double avgPerNode = (double) totalLoad / numMembers;
        if (avgPerNode == 0) {
            avgPerNode = 1;
        }

        double avgLoad = avgPerNode * config.getLoadFactor();
        return Math.ceil(avgLoad);
    }

    public Member locate(String key) {
        if (this.hashRing.size() == 0) {
           throw new EmptyHashRingException();
        }

        long hkey = config.getHash64().hash64(key);
        SortedMap<Long, Member> result = hashRing.tailMap(hkey);
        if (result.size() == 0) {
            result = hashRing.headMap(hkey);
        }

        double avgLoad = averageLoad();

        for (Long fkey : result.keySet()) {
            Member m = hashRing.get(fkey);
            Integer load = loads.get(m);
            if ((double) load + 1 <= avgLoad) {
                return m;
            }
        }

        throw new RuntimeException();
    }

    public void incrLoad(Member m) {
        Integer load = loads.get(m);
        if (load == null) {
            throw new MemberNotFoundException();
        }

        loads.put(m, load + 1);
        totalLoad++;
    }

    public void decrLoad(Member m) {
        Integer load = loads.get(m);
        if (load == null) {
            throw new MemberNotFoundException();
        }

        if (load == 0) {
            return;
        }

        loads.put(m, load - 1);
        totalLoad--;
    }

    public int size() {
        return hashRing.size() / config.getReplicaCount();
    }

    public Collection<Member> members() {
        return new ArrayList<>(loads.keySet());
    }

    public Integer getLoad(Member m) {
        return loads.get(m);
    }

    public Config config() {
      return config;
    }
}
