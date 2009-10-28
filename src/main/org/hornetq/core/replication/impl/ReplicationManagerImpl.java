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
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.hornetq.core.client.impl.FailoverManager;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.CreateReplicationSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationLargeMessageBeingMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationLargemessageEndMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.replication.ReplicationContext;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.SimpleString;

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

   private final FailoverManager failoverManager;

   private RemotingConnection connection;

   private Channel replicatingChannel;

   private boolean started;

   private volatile boolean enabled;

   private final Object replicationLock = new Object();

   private final Executor executor;

   private final ThreadLocal<ReplicationContext> tlReplicationContext = new ThreadLocal<ReplicationContext>();

   private final Queue<ReplicationContext> pendingTokens = new ConcurrentLinkedQueue<ReplicationContext>();

   private final ConcurrentHashSet<ReplicationContext> activeContexts = new ConcurrentHashSet<ReplicationContext>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param replicationConnectionManager
    */
   public ReplicationManagerImpl(final FailoverManager failoverManager, final Executor executor)
   {
      super();
      this.failoverManager = failoverManager;
      this.executor = executor;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#replicate(byte[], org.hornetq.core.replication.ReplicationToken)
    */

   public void appendAddRecord(final byte journalID, final long id, final byte recordType, final EncodingSupport record)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddMessage(journalID, false, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendUpdateRecord(byte, long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendUpdateRecord(final byte journalID,
                                  final long id,
                                  final byte recordType,
                                  final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddMessage(journalID, true, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecord(byte, long, boolean)
    */
   public void appendDeleteRecord(final byte journalID, final long id) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteMessage(journalID, id));
      }
   }

   public void appendAddRecordTransactional(final byte journalID,
                                            final long txID,
                                            final long id,
                                            final byte recordType,
                                            final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddTXMessage(journalID, false, txID, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendUpdateRecordTransactional(byte, long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendUpdateRecordTransactional(final byte journalID,
                                               final long txID,
                                               final long id,
                                               final byte recordType,
                                               final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddTXMessage(journalID, true, txID, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendCommitRecord(byte, long, boolean)
    */
   public void appendCommitRecord(final byte journalID, final long txID) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecordTransactional(byte, long, long, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendDeleteRecordTransactional(final byte journalID,
                                               final long txID,
                                               final long id,
                                               final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecordTransactional(byte, long, long)
    */
   public void appendDeleteRecordTransactional(final byte journalID, final long txID, final long id) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, NullEncoding.instance));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendPrepareRecord(byte, long, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendPrepareRecord(final byte journalID, final long txID, final EncodingSupport transactionData) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPrepareMessage(journalID, txID, transactionData));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendRollbackRecord(byte, long, boolean)
    */
   public void appendRollbackRecord(final byte journalID, final long txID) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#pageClosed(org.hornetq.utils.SimpleString, int)
    */
   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, false));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#pageDeleted(org.hornetq.utils.SimpleString, int)
    */
   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, true));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#pageWrite(org.hornetq.utils.SimpleString, int)
    */
   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageWriteMessage(message, pageNumber));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#largeMessageBegin(byte[])
    */
   public void largeMessageBegin(long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageBeingMessage(messageId));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#largeMessageDelete(long)
    */
   public void largeMessageDelete(long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargemessageEndMessage(messageId, true));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#largeMessageEnd(long)
    */
   public void largeMessageEnd(long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargemessageEndMessage(messageId, false));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#largeMessageWrite(long, byte[])
    */
   public void largeMessageWrite(long messageId, byte[] body)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageWriteMessage(messageId, body));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public synchronized boolean isStarted()
   {
      return started;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public synchronized void start() throws Exception
   {
      connection = failoverManager.getConnection();

      if (connection == null)
      {
         log.warn("Backup server MUST be started before live server. Initialisation will not proceed.");
         throw new HornetQException(HornetQException.ILLEGAL_STATE,
                                    "Backup server MUST be started before live server. Initialisation will not proceed.");
      }

      long channelID = connection.generateChannelID();

      Channel mainChannel = connection.getChannel(1, -1, false);

      replicatingChannel = connection.getChannel(channelID, WINDOW_SIZE, false);

      replicatingChannel.setHandler(responseHandler);

      CreateReplicationSessionMessage replicationStartPackage = new CreateReplicationSessionMessage(channelID,
                                                                                                    WINDOW_SIZE);

      mainChannel.sendBlocking(replicationStartPackage);

      failoverManager.addFailureListener(new FailureListener()
      {
         public void connectionFailed(HornetQException me)
         {
            log.warn("Connection to the backup node failed, removing replication now");
            try
            {
               stop();
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      });

      started = true;

      enabled = true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      enabled = false;

      for (ReplicationContext ctx : activeContexts)
      {
         ctx.complete();
         ctx.flush();
      }

      activeContexts.clear();

      if (replicatingChannel != null)
      {
         replicatingChannel.close();
      }

      started = false;

      if (connection != null)
      {
         connection.destroy();
      }

      connection = null;

      started = false;
   }

   public ReplicationContext getContext()
   {
      ReplicationContext token = tlReplicationContext.get();
      if (token == null)
      {
         token = new ReplicationContextImpl(executor);
         activeContexts.add(token);
         tlReplicationContext.set(token);
      }
      return token;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#addReplicationAction(java.lang.Runnable)
    */
   public void afterReplicated(final Runnable runnable)
   {
      getContext().addReplicationAction(runnable);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#completeToken()
    */
   public void closeContext()
   {
      final ReplicationContext token = tlReplicationContext.get();
      if (token != null)
      {
         // Disassociate thread local
         tlReplicationContext.set(null);
         // Remove from pending tokens as soon as this is complete
         token.addReplicationAction(new Runnable()
         {
            public void run()
            {
               activeContexts.remove(token);
            }
         });
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#getPendingTokens()
    */
   public Set<ReplicationContext> getActiveTokens()
   {
      return activeContexts;
   }

   private void sendReplicatePacket(final Packet packet)
   {
      boolean runItNow = false;

      ReplicationContext repliToken = getContext();
      repliToken.linedUp();

      synchronized (replicationLock)
      {
         if (!enabled)
         {
            // Already replicating channel failed, so just play the action now

            runItNow = true;
         }
         else
         {
            pendingTokens.add(repliToken);

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
      ReplicationContext tokenPolled = pendingTokens.poll();
      if (tokenPolled == null)
      {
         throw new IllegalStateException("Missing replication token on the queue.");
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
      public void handlePacket(final Packet packet)
      {
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
