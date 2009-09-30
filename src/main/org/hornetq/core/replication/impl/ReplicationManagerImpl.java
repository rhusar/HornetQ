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

import org.hornetq.core.client.impl.ConnectionManager;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.CreateReplicationSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.replication.ReplicationToken;

/**
 * A RepplicationManagerImpl
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationManagerImpl implements ReplicationManager
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // TODO: where should this be configured?
   private static final int WINDOW_SIZE = 100 * 1024;

   private final ConnectionManager connectionManager;

   private RemotingConnection connection;

   private Channel replicatingChannel;

   private boolean started;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param replicationConnectionManager
    */
   public ReplicationManagerImpl(ConnectionManager connectionManager)
   {
      super();
      this.connectionManager = connectionManager;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#replicate(byte[], org.hornetq.core.replication.ReplicationToken)
    */
   
   
   public void appendAddRecord(long id, byte recordType, EncodingSupport encodingData)
   {
      replicatingChannel.send(new ReplicationAddMessage(id, recordType, encodingData));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public synchronized boolean isStarted()
   {
      return this.started;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public synchronized void start() throws Exception
   {
      this.started = true;

      connection = connectionManager.getConnection(1);

      long channelID = connection.generateChannelID();

      Channel mainChannel = connection.getChannel(1, -1, false);

      Channel tempChannel = connection.getChannel(channelID, WINDOW_SIZE, false);

      CreateReplicationSessionMessage replicationStartPackage = new CreateReplicationSessionMessage(channelID,
                                                                                                    WINDOW_SIZE);

      mainChannel.sendBlocking(replicationStartPackage);

      this.replicatingChannel = tempChannel;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      if (replicatingChannel != null)
      {
         replicatingChannel.close();
      }

      this.started = false;
      
      if (connection != null)
      {
         connection.destroy();
      }

      connection = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
