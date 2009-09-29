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

package org.hornetq.core.replication.impl;

import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPacket;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.HornetQServer;

/**
 * A ReplicationPacketHandler
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationEndpointImpl implements ReplicationEndpoint
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final HornetQServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public ReplicationEndpointImpl(HornetQServer server)
   {
      this.server = server;
   }

   // Public --------------------------------------------------------
   /* (non-Javadoc)
    * @see org.hornetq.core.remoting.ChannelHandler#handlePacket(org.hornetq.core.remoting.Packet)
    */
   public void handlePacket(Packet packet)
   {

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
