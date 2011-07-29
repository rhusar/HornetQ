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
package org.hornetq.core.client.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.logging.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Aug 16, 2010
 */
public class Topology implements Serializable
{

   private static final long serialVersionUID = -9037171688692471371L;

   private final Set<ClusterTopologyListener> topologyListeners = new HashSet<ClusterTopologyListener>();

   private static final Logger log = Logger.getLogger(Topology.class);

   /** Used to debug operations.
    * 
    *  Someone may argue this is not needed. But it's impossible to debg anything related to topology without knowing what node
    *  or what object missed a Topology update.
    *  
    *  Hence I added some information to locate debugging here. 
    *  */
   private volatile Object owner;

   private volatile Executor executor;

   public Topology(final Object owner)
   {
      this.owner = owner;
      Topology.log.debug("ZZZ III Topology@" + Integer.toHexString(System.identityHashCode(this)) + " CREATE",
                         new Exception("trace")); // Delete this line
   }

   /*
    * topology describes the other cluster nodes that this server knows about:
    *
    * keys are node IDs
    * values are a pair of live/backup transport configurations
    */
   private final Map<String, TopologyMember> topology = new ConcurrentHashMap<String, TopologyMember>();

   public void setExecutor(Executor executor)
   {
      this.executor = executor;
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + "::PPP Adding topology listener " + listener, new Exception("Trace"));
      }
      synchronized (topologyListeners)
      {
         topologyListeners.add(listener);
      }
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + "::PPP Removing topology listener " + listener, new Exception("Trace"));
      }
      synchronized (topologyListeners)
      {
         topologyListeners.remove(listener);
      }
   }

   public  boolean addMember(final String nodeId, final TopologyMember member, final boolean last)
   {
      boolean replaced = false;

      synchronized (this)
      {
         TopologyMember currentMember = topology.get(nodeId);

         if (Topology.log.isDebugEnabled())
         {
            Topology.log.debug(this + "::adding = " + nodeId + ":" + member.getConnector(), new Exception("trace"));
            Topology.log.debug(describe("Before:"));
         }

         if (currentMember == null)
         {
            replaced = true;
            if (Topology.log.isDebugEnabled())
            {
               Topology.log.debug("ZZZ " + this +
                                  " MEMBER WAS NULL, Add member nodeId=" +
                                  nodeId +
                                  " member = " +
                                  member +
                                  " replaced = " +
                                  replaced +
                                  " size = " +
                                  topology.size(), new Exception("trace"));
            }
            topology.put(nodeId, member);
         }
         else
         {
            if (hasChanged(currentMember.getConnector().a, member.getConnector().a) && member.getConnector().a != null)
            {
               currentMember.getConnector().a = member.getConnector().a;
               replaced = true;
            }
            if (hasChanged(currentMember.getConnector().b, member.getConnector().b) && member.getConnector().b != null)
            {
               currentMember.getConnector().b = member.getConnector().b;
               replaced = true;
            }

            if (member.getConnector().a == null)
            {
               member.getConnector().a = currentMember.getConnector().a;
            }
            if (member.getConnector().b == null)
            {
               member.getConnector().b = currentMember.getConnector().b;
            }
         }

         if (Topology.log.isDebugEnabled())
         {
            Topology.log.debug(this + "::Topology updated=" + replaced);
            Topology.log.debug(describe(this + "::After:"));
         }

         if (Topology.log.isDebugEnabled())
         {
            Topology.log.debug("ZZZ " + this +
                               " Add member nodeId=" +
                               nodeId +
                               " member = " +
                               member +
                               " replaced = " +
                               replaced +
                               " size = " +
                               topology.size(), new Exception("trace"));
         }

      }
      
      if (replaced)
      {
         ArrayList<ClusterTopologyListener> copy = copyListeners();
         for (ClusterTopologyListener listener : copy)
         {
            if (Topology.log.isTraceEnabled())
            {
               Topology.log.trace("XXX ZZZ " + this + " informing " + listener + " about node up = " + nodeId);
            }

            listener.nodeUP(nodeId, member.getConnector(), last);
         }
      }

      return replaced;
   }

   /**
    * @return
    */
   private ArrayList<ClusterTopologyListener> copyListeners()
   {
      ArrayList <ClusterTopologyListener> listenersCopy;
      synchronized (topologyListeners)
      {
         listenersCopy = new ArrayList<ClusterTopologyListener>(topologyListeners);
      }
      return listenersCopy;
   }

   public boolean removeMember(final String nodeId)
   {
      TopologyMember member;
      
      synchronized (this)
      {
         member = topology.remove(nodeId);
      }

      if (member != null)
      {
         ArrayList<ClusterTopologyListener> copy = copyListeners();

         for (ClusterTopologyListener listener : copy)
         {
            if (Topology.log.isTraceEnabled())
            {
               Topology.log.trace("XXX ZZZ " + this + " informing " + listener + " about node down = " + nodeId);
            }
            listener.nodeDown(nodeId);
         }
      }

      if (Topology.log.isDebugEnabled())
      {
         Topology.log.debug("ZZZ " + this +
                            " removing nodeID=" +
                            nodeId +
                            ", result=" +
                            member +
                            ", size = " +
                            topology.size(), new Exception("trace"));
      }

      return member != null;
   }

   /**
    * it will send all the member updates to listeners, independently of being changed or not
    * @param nodeID
    * @param member
    */
   public void sendMemberToListeners(String nodeID, TopologyMember member)
   {
      // To make sure it was updated
      addMember(nodeID, member, false);
      
      ArrayList<ClusterTopologyListener> copy = copyListeners();

      // Now force sending it
      for (ClusterTopologyListener listener : copy)
      {
         if (log.isDebugEnabled())
         {
            log.debug("Informing client listener " + listener +
                      " about itself node " +
                      nodeID +
                      " with connector=" +
                      member.getConnector());
         }
         listener.nodeUP(nodeID, member.getConnector(), false);
      }
   }

   public synchronized void sendTopology(final ClusterTopologyListener listener)
   {
      int count = 0;

      Map<String, TopologyMember> copy;

      synchronized (this)
      {
         copy = new HashMap<String, TopologyMember>(topology);
      }

      for (Map.Entry<String, TopologyMember> entry : copy.entrySet())
      {
         listener.nodeUP(entry.getKey(), entry.getValue().getConnector(), ++count == copy.size());
      }
   }

   public TopologyMember getMember(final String nodeID)
   {
      return topology.get(nodeID);
   }

   public boolean isEmpty()
   {
      return topology.isEmpty();
   }

   public Collection<TopologyMember> getMembers()
   {
      ArrayList<TopologyMember> members;
      synchronized (this)
      {
         members = new ArrayList<TopologyMember>(topology.values());
      }
      return members;
   }

   public synchronized int nodes()
   {
      int count = 0;
      for (TopologyMember member : topology.values())
      {
         if (member.getConnector().a != null)
         {
            count++;
         }
         if (member.getConnector().b != null)
         {
            count++;
         }
      }
      return count;
   }

   public synchronized String describe()
   {
      return describe("");
   }

   public synchronized String describe(final String text)
   {

      String desc = text + "\n";
      for (Entry<String, TopologyMember> entry : new HashMap<String, TopologyMember>(topology).entrySet())
      {
         desc += "\t" + entry.getKey() + " => " + entry.getValue() + "\n";
      }
      desc += "\t" + "nodes=" + nodes() + "\t" + "members=" + members();
      return desc;
   }

   public void clear()
   {
      if (Topology.log.isDebugEnabled())
      {
         Topology.log.debug("ZZZ III " + this + "::clear", new Exception("trace"));
      }
      // topology.clear();
   }

   public int members()
   {
      return topology.size();
   }

   public void setOwner(final Object owner)
   {
      this.owner = owner;
   }

   private boolean hasChanged(final TransportConfiguration currentConnector, final TransportConfiguration connector)
   {
      return currentConnector == null && connector != null ||
             currentConnector != null &&
             !currentConnector.equals(connector);
   }

   public TransportConfiguration getBackupForConnector(final TransportConfiguration connectorConfiguration)
   {
      for (TopologyMember member : topology.values())
      {
         if (member.getConnector().a != null && member.getConnector().a.equals(connectorConfiguration))
         {
            return member.getConnector().b;
         }
      }
      return null;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      if (owner == null)
      {
         return super.toString();
      }
      else
      {
         return "Topology@" + Integer.toHexString(System.identityHashCode(this)) + "[owner=" + owner + "]";
      }
   }

}
