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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.hornetq.core.client.impl.ConnectionManager;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.CreateReplicationSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;
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
   private static final Logger log = Logger.getLogger(ReplicationManagerImpl.class);

   // Attributes ----------------------------------------------------

   // TODO: where should this be configured?
   private static final int WINDOW_SIZE = 1024 * 1024;

   private final ResponseHandler responseHandler = new ResponseHandler();

   private final ConnectionManager connectionManager;

   private RemotingConnection connection;

   private Channel replicatingChannel;

   private boolean started;

   private boolean playedResponsesOnFailure;

   private final Object replicationLock = new Object();

   private final Executor executor;

   private final ThreadLocal<ReplicationToken> repliToken = new ThreadLocal<ReplicationToken>();

   private final Queue<ReplicationToken> pendingTokens = new ConcurrentLinkedQueue<ReplicationToken>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param replicationConnectionManager
    */
   public ReplicationManagerImpl(final ConnectionManager connectionManager, final Executor executor)
   {
      super();
      this.connectionManager = connectionManager;
      this.executor = executor;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#replicate(byte[], org.hornetq.core.replication.ReplicationToken)
    */

   public void appendAddRecord(final byte journalID, final long id, final byte recordType, final EncodingSupport record)
   {
      sendReplicatePacket(new ReplicationAddMessage(journalID, false, id, recordType, record));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendUpdateRecord(byte, long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendUpdateRecord(byte journalID, long id, byte recordType, EncodingSupport record) throws Exception
   {
      sendReplicatePacket(new ReplicationAddMessage(journalID, true, id, recordType, record));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecord(byte, long, boolean)
    */
   public void appendDeleteRecord(byte journalID, long id) throws Exception
   {
      sendReplicatePacket(new ReplicationDeleteMessage(journalID, id));
   }

   public void appendAddRecordTransactional(byte journalID, long txID, long id, byte recordType, EncodingSupport record) throws Exception
   {
      sendReplicatePacket(new ReplicationAddTXMessage(journalID, false, txID, id, recordType, record));
   }

   
   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendUpdateRecordTransactional(byte, long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendUpdateRecordTransactional(byte journalID,
                                               long txID,
                                               long id,
                                               byte recordType,
                                               EncodingSupport record) throws Exception
   {
      sendReplicatePacket(new ReplicationAddTXMessage(journalID, true, txID, id, recordType, record));
   }

   
   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendCommitRecord(byte, long, boolean)
    */
   public void appendCommitRecord(byte journalID, long txID) throws Exception
   {
      sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecordTransactional(byte, long, long, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendDeleteRecordTransactional(byte journalID, long txID, long id, EncodingSupport record) throws Exception
   {
      sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, record));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecordTransactional(byte, long, long)
    */
   public void appendDeleteRecordTransactional(byte journalID, long txID, long id) throws Exception
   {
      sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, NullEncoding.instance));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendPrepareRecord(byte, long, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendPrepareRecord(byte journalID, long txID, EncodingSupport transactionData) throws Exception
   {
      sendReplicatePacket(new ReplicationPrepareMessage(journalID, txID, transactionData));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendRollbackRecord(byte, long, boolean)
    */
   public void appendRollbackRecord(byte journalID, long txID) throws Exception
   {
      sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID));
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
      connection = connectionManager.getConnection(1);

      long channelID = connection.generateChannelID();

      Channel mainChannel = connection.getChannel(1, -1, false);

      this.replicatingChannel = connection.getChannel(channelID, WINDOW_SIZE, false);

      this.replicatingChannel.setHandler(this.responseHandler);

      CreateReplicationSessionMessage replicationStartPackage = new CreateReplicationSessionMessage(channelID,
                                                                                                    WINDOW_SIZE);

      mainChannel.sendBlocking(replicationStartPackage);

      this.started = true;
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

   public ReplicationToken getReplicationToken()
   {
      ReplicationToken token = repliToken.get();
      if (token == null)
      {
         token = new ReplicationTokenImpl(executor);
         repliToken.set(token);
      }
      return token;
   }

   private void sendReplicatePacket(final Packet packet)
   {
      boolean runItNow = false;

      ReplicationToken repliToken = getReplicationToken();
      repliToken.linedUp();

      synchronized (replicationLock)
      {
         if (playedResponsesOnFailure)
         {
            // Already replicating channel failed, so just play the action now

            runItNow = true;
         }
         else
         {
            pendingTokens.add(repliToken);

            // TODO: Should I use connect.write directly here?
            replicatingChannel.send(packet);
         }
      }

      // Execute outside lock

      if (runItNow)
      {
         repliToken.replicated();
      }
   }

   private void replicated()
   {
      ReplicationToken tokenPolled = pendingTokens.poll();
      if (tokenPolled == null)
      {
         // We should debug the logs if this happens
         log.warn("Missing replication token on the stack. There is a bug on the ReplicatoinManager since this was not supposed to happen");
      }
      else
      {
         tokenPolled.replicated();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected class ResponseHandler implements ChannelHandler
   {
      /* (non-Javadoc)
       * @see org.hornetq.core.remoting.ChannelHandler#handlePacket(org.hornetq.core.remoting.Packet)
       */
      public void handlePacket(Packet packet)
      {
         System.out.println("HandlePacket on client");
         if (packet.getType() == PacketImpl.REPLICATION_RESPONSE)
         {
            replicated();
         }
      }

   }

   private static class NullEncoding implements EncodingSupport
   {

      static NullEncoding instance = new NullEncoding();
      
      public void decode(final HornetQBuffer buffer)
      {
      }

      public void encode(final HornetQBuffer buffer)
      {
      }

      public int getEncodeSize()
      {
         return 0;
      }

   }

}
