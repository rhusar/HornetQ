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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.logging.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author Clebert Suconic
 *         Created Aug 16, 2010
 */
public class Topology implements Serializable
{

   private static final int BACKOF_TIMEOUT = 500;

   private static final long serialVersionUID = -9037171688692471371L;

   private final Set<ClusterTopologyListener> topologyListeners = new HashSet<ClusterTopologyListener>();

   private static final Logger log = Logger.getLogger(Topology.class);

   private transient HashMap<String, Pair<Long, Integer>> mapBackof = new HashMap<String, Pair<Long, Integer>>();

   private Executor executor = null;

   /** Used to debug operations.
    * 
    *  Someone may argue this is not needed. But it's impossible to debg anything related to topology without knowing what node
    *  or what object missed a Topology update.
    *  
    *  Hence I added some information to locate debugging here. 
    *  */
   private volatile Object owner;

   /**
    * topology describes the other cluster nodes that this server knows about:
    *
    * keys are node IDs
    * values are a pair of live/backup transport configurations
    */
   private final Map<String, TopologyMember> mapTopology = new ConcurrentHashMap<String, TopologyMember>();

   private final Map<String, Long> mapDelete = new ConcurrentHashMap<String, Long>();

   public Topology(final Object owner)
   {
      this.owner = owner;
      if (log.isTraceEnabled())
      {
         Topology.log.trace("Topology@" + Integer.toHexString(System.identityHashCode(this)) + " CREATE",
                            new Exception("trace"));
      }
   }

   public void setExecutor(final Executor executor)
   {
      this.executor = executor;
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + "::Adding topology listener " + listener, new Exception("Trace"));
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
         log.debug(this + "::Removing topology listener " + listener, new Exception("Trace"));
      }
      synchronized (topologyListeners)
      {
         topologyListeners.remove(listener);
      }
   }

   /** This is called by the server when the node is activated from backup state. It will always succeed */
   public void updateAsLive(final String nodeId, final TopologyMember memberInput)
   {
      synchronized (this)
      {
         if (log.isDebugEnabled())
         {
            log.info(this + "::Live node " + nodeId + "=" + memberInput);
         }
         memberInput.setUniqueEventID(System.currentTimeMillis());
         mapTopology.remove(nodeId);
         mapTopology.put(nodeId, memberInput);
         sendMemberUp(memberInput.getUniqueEventID(), nodeId, memberInput);
      }
   }

   /** This is called by the server when the node is activated from backup state. It will always succeed */
   public TopologyMember updateBackup(final String nodeId, final TopologyMember memberInput)
   {
      if (log.isTraceEnabled())
      {
         log.trace(this + "::updateBackup::" + nodeId + ", memberInput=" + memberInput);
      }

      synchronized (this)
      {
         TopologyMember currentMember = getMember(nodeId);
         if (currentMember == null)
         {
            log.warn("There's no live to be updated on backup update, node=" + nodeId + " memberInput=" + memberInput,
                     new Exception("trace"));

            currentMember = memberInput;
            mapTopology.put(nodeId, currentMember);
         }

         TopologyMember newMember = new TopologyMember(currentMember.getConnector().a, memberInput.getConnector().b);
         newMember.setUniqueEventID(System.currentTimeMillis());
         mapTopology.remove(nodeId);
         mapTopology.put(nodeId, newMember);
         sendMemberUp(newMember.getUniqueEventID(), nodeId, newMember);

         return newMember;
      }
   }

   /**
    * 
    * @param <p>uniqueIdentifier an unique identifier for when the change was made
    *           We will use current time millis for starts, and a ++ of that number for shutdown. </p> 
    * @param nodeId
    * @param memberInput
    * @return
    */
   public boolean updateMember(final long uniqueEventID, final String nodeId, final TopologyMember memberInput)
   {
      
      if (memberInput.getConnector().a == null && memberInput.getConnector().b != null)
      {
         updateBackup(nodeId, memberInput);
         return true;
      }

      Long deleteTme = mapDelete.get(nodeId);
      if (deleteTme != null && uniqueEventID < deleteTme)
      {
         log.debug("Update uniqueEvent=" + uniqueEventID +
                   ", nodeId=" +
                   nodeId +
                   ", memberInput=" +
                   memberInput +
                   " being rejected as there was a delete done after that");
         return false;
      }

      synchronized (this)
      {
         TopologyMember currentMember = mapTopology.get(nodeId);

         if (currentMember == null)
         {
            if (Topology.log.isDebugEnabled())
            {
               Topology.log.debug(this + "::NewMemeberAdd " +
                                  this +
                                  " MEMBER WAS NULL, Add member nodeId=" +
                                  nodeId +
                                  " member = " +
                                  memberInput +
                                  " size = " +
                                  mapTopology.size(), new Exception("trace"));
            }
            memberInput.setUniqueEventID(uniqueEventID);
            mapTopology.put(nodeId, memberInput);
            sendMemberUp(uniqueEventID, nodeId, memberInput);
            return true;
         }
         else
         {
            if (uniqueEventID > currentMember.getUniqueEventID())
            {
               if (log.isDebugEnabled())
               {
                  log.debug(this + "::updated currentMember=nodeID=" +
                            nodeId +
                            currentMember +
                            " of memberInput=" +
                            memberInput);
               }

               TopologyMember newMember = new TopologyMember(memberInput.getConnector().a, memberInput.getConnector().b);
               newMember.setUniqueEventID(uniqueEventID);
               mapTopology.remove(nodeId);
               mapTopology.put(nodeId, newMember);
               sendMemberUp(uniqueEventID, nodeId, newMember);

               return true;
            }
            else
            {
               return false;
            }
         }

      }
   }

   /**
    * @param nodeId
    * @param memberToSend
    */
   private void sendMemberUp(final long uniqueEventID, final String nodeId, final TopologyMember memberToSend)
   {
      final ArrayList<ClusterTopologyListener> copy = copyListeners();

      if (log.isTraceEnabled())
      {
         log.trace(this + "::prepare to send " + nodeId + " to " + copy.size() + " elements");
      }

      execute(new Runnable()
      {
         public void run()
         {
            for (ClusterTopologyListener listener : copy)
            {
               if (Topology.log.isTraceEnabled())
               {
                  Topology.log.trace(Topology.this + " informing " +
                                     listener +
                                     " about node up = " +
                                     nodeId +
                                     " connector = " +
                                     memberToSend.getConnector());
               }

               try
               {
                  listener.nodeUP(uniqueEventID, nodeId, memberToSend.getConnector(), false);
               }
               catch (Throwable e)
               {
                  log.warn(e.getMessage(), e);
               }
            }
         }
      });
   }

   /**
    * @param nodeId
    * @param backOfData
    */
   private boolean testBackof(final String nodeId)
   {
      Pair<Long, Integer> backOfData = mapBackof.get(nodeId);

      if (backOfData != null)
      {
         backOfData.b += 1;

         long timeDiff = System.currentTimeMillis() - backOfData.a;

         // To prevent a loop where nodes are being considered down and up
         if (backOfData.b > 5 && timeDiff < BACKOF_TIMEOUT)
         {

            // The cluster may get in loop without this..
            // Case one node is stll sending nodeDown while another member is sending nodeUp
            log.warn(backOfData.b + ", The topology controller identified a blast events and it's interrupting the flow of the loop, nodeID=" +
                              nodeId +
                              ", topologyInstance=" +
                              this,
                     new Exception("this exception is just to trace location"));
            return false;
         }
         else if (timeDiff < BACKOF_TIMEOUT)
         {
            log.warn(this + "::Simple blast of " + nodeId, new Exception("this exception is just to trace location"));
         }
         else if (timeDiff >= BACKOF_TIMEOUT)
         {
            mapBackof.remove(nodeId);
         }
      }

      return true;
   }

   /**
    * @return
    */
   private ArrayList<ClusterTopologyListener> copyListeners()
   {
      ArrayList<ClusterTopologyListener> listenersCopy;
      synchronized (topologyListeners)
      {
         listenersCopy = new ArrayList<ClusterTopologyListener>(topologyListeners);
      }
      return listenersCopy;
   }

   public boolean removeMember(final long uniqueEventID, final String nodeId)
   {
      TopologyMember member;

      synchronized (this)
      {
         member = mapTopology.get(nodeId);
         if (member != null)
         {
            if (member.getUniqueEventID() > uniqueEventID)
            {
               log.info("The removeMember was issued before the node " + nodeId + " was started, ignoring call");
               member = null;
            }
            else
            {
               mapDelete.put(nodeId, uniqueEventID);
               member = mapTopology.remove(nodeId);
            }
         }
      }

      if (Topology.log.isDebugEnabled())
      {
         Topology.log.debug("removeMember " + this +
                            " removing nodeID=" +
                            nodeId +
                            ", result=" +
                            member +
                            ", size = " +
                            mapTopology.size(), new Exception("trace"));
      }

      if (member != null)
      {
         final ArrayList<ClusterTopologyListener> copy = copyListeners();

         execute(new Runnable()
         {
            public void run()
            {
               for (ClusterTopologyListener listener : copy)
               {
                  if (Topology.log.isTraceEnabled())
                  {
                     Topology.log.trace(this + " informing " + listener + " about node down = " + nodeId);
                  }
                  try
                  {
                     listener.nodeDown(uniqueEventID, nodeId);
                  }
                  catch (Exception e)
                  {
                     log.warn(e.getMessage(), e);
                  }
               }
            }
         });

      }
      return member != null;
   }

   protected void execute(final Runnable runnable)
   {
      if (executor != null)
      {
         executor.execute(runnable);
      }
      else
      {
         runnable.run();
      }
   }

   /**
    * it will send the member to its listeners
    * @param nodeID
    * @param member
    */
   public void sendMember(final String nodeID)
   {
      final TopologyMember member = getMember(nodeID);

      final ArrayList<ClusterTopologyListener> copy = copyListeners();

      execute(new Runnable()
      {
         public void run()
         {
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
               listener.nodeUP(member.getUniqueEventID(), nodeID, member.getConnector(), false);
            }
         }
      });
   }

   public synchronized void sendTopology(final ClusterTopologyListener listener)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + " is sending topology to " + listener);
      }

      execute(new Runnable()
      {
         public void run()
         {
            int count = 0;

            final Map<String, TopologyMember> copy;

            synchronized (Topology.this)
            {
               copy = new HashMap<String, TopologyMember>(mapTopology);
            }

            for (Map.Entry<String, TopologyMember> entry : copy.entrySet())
            {
               if (log.isDebugEnabled())
               {
                  log.debug(Topology.this + " sending " +
                            entry.getKey() +
                            " / " +
                            entry.getValue().getConnector() +
                            " to " +
                            listener);
               }
               listener.nodeUP(entry.getValue().getUniqueEventID(),
                               entry.getKey(),
                               entry.getValue().getConnector(),
                               ++count == copy.size());
            }
         }
      });
   }

   public synchronized TopologyMember getMember(final String nodeID)
   {
      return mapTopology.get(nodeID);
   }

   public synchronized boolean isEmpty()
   {
      return mapTopology.isEmpty();
   }

   public Collection<TopologyMember> getMembers()
   {
      ArrayList<TopologyMember> members;
      synchronized (this)
      {
         members = new ArrayList<TopologyMember>(mapTopology.values());
      }
      return members;
   }

   public synchronized int nodes()
   {
      int count = 0;
      for (TopologyMember member : mapTopology.values())
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
      for (Entry<String, TopologyMember> entry : new HashMap<String, TopologyMember>(mapTopology).entrySet())
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
         Topology.log.debug(this + "::clear", new Exception("trace"));
      }
      mapTopology.clear();
   }

   public int members()
   {
      return mapTopology.size();
   }

   /** The owner exists mainly for debug purposes.
    *  When enabling logging and tracing, the Topology updates will include the owner, what will enable to identify
    *  what instances are receiving the updates, what will enable better debugging.*/
   public void setOwner(final Object owner)
   {
      this.owner = owner;
   }

   private boolean hasChanged(final String debugInfo, final TransportConfiguration a, final TransportConfiguration b)
   {
      boolean changed = a == null && b != null || a != null && b != null && !a.equals(b);

      if (log.isTraceEnabled())
      {

         log.trace(this + "::Validating current=" +
                   a +
                   " != input=" +
                   b +
                   (changed ? " and it has changed" : " and it didn't change") +
                   ", for validation of " +
                   debugInfo);
      }

      return changed;
   }

   public TransportConfiguration getBackupForConnector(final TransportConfiguration connectorConfiguration)
   {
      for (TopologyMember member : mapTopology.values())
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
         return "Topology@" + Integer.toHexString(System.identityHashCode(this));
      }
      else
      {
         return "Topology@" + Integer.toHexString(System.identityHashCode(this)) + "[owner=" + owner + "]";
      }
   }

}
