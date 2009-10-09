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

package org.hornetq.tests.integration.replication;

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ConnectionManager;
import org.hornetq.core.client.impl.ConnectionManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.impl.PagedMessageImpl;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.replication.impl.ReplicatedJournal;
import org.hornetq.core.replication.impl.ReplicationManagerImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.SimpleString;

/**
 * A ReplicationTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ThreadFactory tFactory;

   private ExecutorService executor;

   private ConnectionManager connectionManager;

   private ScheduledExecutorService scheduledExecutor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testBasicConnection() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(connectionManager, executor);
         manager.start();
         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   public void testConnectIntoNonBackup() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(false);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(connectionManager, executor);
         try
         {
            manager.start();
            fail("Exception was expected");
         }
         catch (HornetQException expected)
         {
         }

         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   public void testSendPackets() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(connectionManager, executor);
         manager.start();

         Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

         replicatedJournal.appendPrepareRecord(1, new FakeData(), false);

         replicatedJournal.appendAddRecord(1, (byte)1, new FakeData(), false);
         replicatedJournal.appendUpdateRecord(1, (byte)2, new FakeData(), false);
         replicatedJournal.appendDeleteRecord(1, false);
         replicatedJournal.appendAddRecordTransactional(2, 2, (byte)1, new FakeData());
         replicatedJournal.appendUpdateRecordTransactional(2, 2, (byte)2, new FakeData());
         replicatedJournal.appendCommitRecord(2, false);

         replicatedJournal.appendDeleteRecordTransactional(3, 4, new FakeData());
         replicatedJournal.appendPrepareRecord(3, new FakeData(), false);
         replicatedJournal.appendRollbackRecord(3, false);

         blockOnReplication(manager);

         assertEquals(1, manager.getActiveTokens().size());

         manager.completeToken();

         for (int i = 0; i < 100; i++)
         {
            // This is asynchronous. Have to wait completion
            if (manager.getActiveTokens().size() == 0)
            {
               break;
            }
            Thread.sleep(1);
         }

         assertEquals(0, manager.getActiveTokens().size());

         ServerMessage msg = new ServerMessageImpl();

         SimpleString dummy = new SimpleString("dummy");
         msg.setDestination(dummy);
         msg.setBody(ChannelBuffers.wrappedBuffer(new byte[10]));

         replicatedJournal.appendAddRecordTransactional(23, 24, (byte)1, new FakeData());

         PagedMessage pgmsg = new PagedMessageImpl(msg, -1);
         manager.pageWrite(pgmsg, 1);
         manager.pageWrite(pgmsg, 2);
         manager.pageWrite(pgmsg, 3);
         manager.pageWrite(pgmsg, 4);

         blockOnReplication(manager);

         PagingManager pagingManager = createPageManager(server.getStorageManager(),
                                                         server.getConfiguration(),
                                                         server.getExecutorFactory(),
                                                         server.getAddressSettingsRepository());
         
         PagingStore store = pagingManager.getPageStore(dummy);
         store.start();
         assertEquals(5, store.getNumberOfPages());
         store.stop();
         
         manager.pageDeleted(dummy, 1);
         manager.pageDeleted(dummy, 2);
         manager.pageDeleted(dummy, 3);
         manager.pageDeleted(dummy, 4);
         manager.pageDeleted(dummy, 5);
         manager.pageDeleted(dummy, 6);
         

         blockOnReplication(manager);
         
         store.start();
         
         assertEquals(0, store.getNumberOfPages());

         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   /**
    * @param manager
    * @return
    */
   private void blockOnReplication(ReplicationManagerImpl manager) throws Exception
   {
      final CountDownLatch latch = new CountDownLatch(1);
      manager.afterReplicated(new Runnable()
      {

         public void run()
         {
            latch.countDown();
         }

      });
      
      assertTrue(latch.await(30, TimeUnit.SECONDS));
   }

   public void testNoActions() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(connectionManager, executor);
         manager.start();

         Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

         replicatedJournal.appendPrepareRecord(1, new FakeData(), false);

         final CountDownLatch latch = new CountDownLatch(1);
         manager.afterReplicated(new Runnable()
         {

            public void run()
            {
               latch.countDown();
            }

         });
         assertTrue(latch.await(1, TimeUnit.SECONDS));
         assertEquals(1, manager.getActiveTokens().size());

         manager.completeToken();

         for (int i = 0; i < 100; i++)
         {
            // This is asynchronous. Have to wait completion
            if (manager.getActiveTokens().size() == 0)
            {
               break;
            }
            Thread.sleep(1);
         }

         assertEquals(0, manager.getActiveTokens().size());
         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   class FakeData implements EncodingSupport
   {

      public void decode(HornetQBuffer buffer)
      {
      }

      public void encode(HornetQBuffer buffer)
      {
         buffer.writeBytes(new byte[5]);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return 5;
      }

   }

   // Package protected ---------------------------------------------
   class LocalRemotingServiceImpl extends RemotingServiceImpl
   {

      public LocalRemotingServiceImpl(Configuration config,
                                      HornetQServer server,
                                      ExecutorFactory executorFactory,
                                      ManagementService managementService,
                                      Executor threadPool,
                                      ScheduledExecutorService scheduledThreadPool,
                                      int managementConnectorID)
      {
         super(config,
               server,
               executorFactory,
               managementService,
               threadPool,
               scheduledThreadPool,
               managementConnectorID);
      }

      protected ChannelHandler createHandler(RemotingConnection conn, Channel channel)
      {
         return super.createHandler(conn, channel);
      }

   }

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      tFactory = new HornetQThreadFactory("HornetQ-ReplicationTest", false);

      executor = Executors.newCachedThreadPool(tFactory);

      scheduledExecutor = new ScheduledThreadPoolExecutor(10, tFactory);

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          new HashMap<String, Object>(),
                                                                          randomString());

      List<Interceptor> interceptors = new ArrayList<Interceptor>();

      connectionManager = new ConnectionManagerImpl(null,
                                                    connectorConfig,
                                                    null,
                                                    false,
                                                    1,
                                                    ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                    ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                    ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                                    0,
                                                    1.0d,
                                                    0,
                                                    false,
                                                    executor,
                                                    scheduledExecutor,
                                                    interceptors);

   }

   protected void tearDown() throws Exception
   {

      executor.shutdown();

      scheduledExecutor.shutdown();

      tFactory = null;

      connectionManager = null;

      scheduledExecutor = null;

      super.tearDown();

   }

   protected PagingManager createPageManager(StorageManager storageManager,
                                             Configuration configuration,
                                             ExecutorFactory executorFactory,
                                             HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
   {

      PagingManager paging = new PagingManagerImpl(new PagingStoreFactoryNIO(configuration.getPagingDirectory(),
                                                                             executorFactory),
                                                   storageManager,
                                                   addressSettingsRepository,
                                                   false);

      paging.start();
      return paging;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   static class FakeJournal implements Journal
   {

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean)
       */
      public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
       */
      public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, byte[])
       */
      public void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
       */
      public void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean)
       */
      public void appendCommitRecord(long txID, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean)
       */
      public void appendDeleteRecord(long id, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, byte[])
       */
      public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, org.hornetq.core.journal.EncodingSupport)
       */
      public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long)
       */
      public void appendDeleteRecordTransactional(long txID, long id) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, org.hornetq.core.journal.EncodingSupport, boolean)
       */
      public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, byte[], boolean)
       */
      public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean)
       */
      public void appendRollbackRecord(long txID, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean)
       */
      public void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
       */
      public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, byte[])
       */
      public void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
       */
      public void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#getAlignment()
       */
      public int getAlignment() throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#load(org.hornetq.core.journal.LoaderCallback)
       */
      public long load(LoaderCallback reloadManager) throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#load(java.util.List, java.util.List, org.hornetq.core.journal.TransactionFailureCallback)
       */
      public long load(List<RecordInfo> committedRecords,
                       List<PreparedTransactionInfo> preparedTransactions,
                       TransactionFailureCallback transactionFailure) throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#perfBlast(int)
       */
      public void perfBlast(int pages) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQComponent#isStarted()
       */
      public boolean isStarted()
      {

         return false;
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

   }
}
