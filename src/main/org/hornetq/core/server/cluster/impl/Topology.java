/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.core.server.cluster.impl;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Aug 16, 2010
 */
public class Topology
{
   /*
    * topology describes the other cluster nodes that this server knows about:
    *
    * keys are node IDs
    * values are a pair of live/backup transport configurations
    */
   private Map<String, TopologyMember> topology = new HashMap<String, TopologyMember>();

   public synchronized void addMember(String nodeId, TopologyMember member)
   {
      topology.put(nodeId, member);
   }

   public synchronized void removeMember(String nodeId)
   {
      topology.remove(nodeId);
   }

   public synchronized void fireListeners(ClusterTopologyListener listener)
   {
      int count = 0;
      for (Map.Entry<String, TopologyMember> entry : topology.entrySet())
      {
         listener.nodeUP(entry.getKey(), entry.getValue().getConnector(), ++count == topology.size(), entry.getValue().getDistance());
      }
   }

   public TopologyMember getMember(String nodeID)
   {
      return topology.get(nodeID);
   }

   public boolean isEmpty()
   {
      return topology.isEmpty();
   }

   public Collection<TopologyMember> getMembers()
   {
      return topology.values();
   }

   public int size()
   {
      return topology.size();
   }
}
