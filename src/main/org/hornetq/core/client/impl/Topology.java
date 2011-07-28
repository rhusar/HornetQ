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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.logging.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Aug 16, 2010
 */
public class Topology implements Serializable
{
   
   /**
    * 
    */
   private static final long serialVersionUID = -9037171688692471371L;

   

   private static final Logger log = Logger.getLogger(Topology.class);
   
   /** Used to debug operations.
    * 
    *  Someone may argue this is not needed. But it's impossible to debg anything related to topology without knowing what node
    *  or what object missed a Topology update.
    *  
    *  Hence I added some information to locate debugging here. 
    *  */
   private final Object owner;
   
   
   public Topology(final Object owner)
   {
      this.owner = owner;
      log.debug("ZZZ III Topology@" + Integer.toHexString(System.identityHashCode(this)) + " CREATE", new Exception ("trace")); // Delete this line
   }

   /*
    * topology describes the other cluster nodes that this server knows about:
    *
    * keys are node IDs
    * values are a pair of live/backup transport configurations
    */
   private Map<String, TopologyMember> topology = new ConcurrentHashMap<String, TopologyMember>();

   private boolean debug = log.isDebugEnabled();

   public synchronized boolean addMember(String nodeId, TopologyMember member)
   {
      boolean replaced = false;
      TopologyMember currentMember = topology.get(nodeId);
      if (debug)
      {
         log.debug(this + "::adding = " + nodeId + ":" + member.getConnector(), new Exception ("trace"));
         log.debug(describe("Before:"));
      }
      if(currentMember == null)
      {
         replaced = true;
        if (log.isDebugEnabled())
         {
            log.debug("ZZZ " + this + " MEMBER WAS NULL, Add member nodeId=" + nodeId + " member = " + member + " replaced = " + replaced + " size = " + topology.size(), new Exception ("trace"));
         }
         topology.put(nodeId, member);
      }
      else
      {
         if(hasChanged(currentMember.getConnector().a, member.getConnector().a) && member.getConnector().a != null)
         {
            currentMember.getConnector().a =  member.getConnector().a;
            replaced = true;
         }
         if(hasChanged(currentMember.getConnector().b, member.getConnector().b) && member.getConnector().b != null)
         {
            currentMember.getConnector().b =  member.getConnector().b;
            replaced = true;
         }

         if(member.getConnector().a == null)
         {
            member.getConnector().a = currentMember.getConnector().a;
         }
         if(member.getConnector().b == null)
         {
            member.getConnector().b = currentMember.getConnector().b;
         }
      }
      if(debug)
      {
         log.debug(this + "::Topology updated=" + replaced);
         log.debug(describe(this + "::After:"));
      }
      
      if (log.isDebugEnabled())
      {
         log.debug("ZZZ " + this + " Add member nodeId=" + nodeId + " member = " + member + " replaced = " + replaced + " size = " + topology.size(), new Exception ("trace"));
      }
      
      return replaced;
   }

   public synchronized boolean removeMember(String nodeId)
   {
      TopologyMember member = topology.remove(nodeId);
      if (log.isDebugEnabled())
      {
         log.debug("ZZZ " + this + " removing nodeID=" + nodeId + ", result=" + member + ", size = " + topology.size(), new Exception ("trace"));
      }
      return (member != null);
   }

   public void sendTopology(ClusterTopologyListener listener)
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
         
         // ARRUMAR ISSO
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

   public synchronized String describe(String text)
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
      if (log.isDebugEnabled())
      {
         log.debug("ZZZ " + this + "::clear", new Exception ("trace"));
      }
      topology.clear();
   }

   public int members()
   {
      return topology.size();
   }

   private boolean hasChanged(TransportConfiguration currentConnector, TransportConfiguration connector)
   {
      return (currentConnector == null && connector != null) || (currentConnector != null && !currentConnector.equals(connector));
   }

   public TransportConfiguration getBackupForConnector(TransportConfiguration connectorConfiguration)
   {
      for (TopologyMember member : topology.values())
      {
         if(member.getConnector().a != null && member.getConnector().a.equals(connectorConfiguration))
         {
            return member.getConnector().b;  
         }
      }
      return null;
   }

   public void setDebug(boolean b)
   {
      debug = b;
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
