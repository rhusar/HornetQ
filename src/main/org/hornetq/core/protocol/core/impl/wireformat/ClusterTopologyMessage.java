/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.protocol.core.impl.wireformat;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClusterTopologyMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ClusterTopologyMessage.class);

   // Attributes ----------------------------------------------------

   private List<Pair<TransportConfiguration, TransportConfiguration>> topology;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClusterTopologyMessage(final List<Pair<TransportConfiguration, TransportConfiguration>> topology)
   {
      super(PacketImpl.CLUSTER_TOPOLOGY);

      this.topology = topology;
   }

   public ClusterTopologyMessage()
   {
      super(PacketImpl.CLUSTER_TOPOLOGY);
   }

   // Public --------------------------------------------------------


   public List<Pair<TransportConfiguration, TransportConfiguration>> getTopology()
   {
      return topology;
   }
   

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(topology.size());
      for (Pair<TransportConfiguration, TransportConfiguration> pair: topology)
      {
         pair.a.encode(buffer);
         if (pair.b != null)
         {
            buffer.writeBoolean(true);
            pair.b.encode(buffer);
         }
         else
         {
            buffer.writeBoolean(false);
         }
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int size = buffer.readInt();
      topology = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      for (int i = 0; i < size; i++)
      {
         TransportConfiguration a = new TransportConfiguration();
         a.decode(buffer);
         boolean hasBackup = buffer.readBoolean();
         TransportConfiguration b;
         if (hasBackup)
         {
            b = new TransportConfiguration();
            b.decode(buffer);
         }
         else
         {
            b = null;
         }
         Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(a, b);
         topology.add(pair);
      }
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
