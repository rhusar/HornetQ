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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.core.buffers.HornetQChannelBuffers;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.FailoverManager;
import org.hornetq.core.client.impl.FailoverManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.impl.PagedMessageImpl;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
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

   private ScheduledExecutorService scheduledExecutor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testBasicConnection() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      FailoverManager failoverManager = createFailoverManager();

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);
         manager.start();
         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   public void testInvalidJournal() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      FailoverManager failoverManager = createFailoverManager();

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);
         manager.start();
         try
         {
            manager.compareJournals(new JournalLoadInformation[] { new JournalLoadInformation(2, 2),
                                                                  new JournalLoadInformation(2, 2) });
            fail("Exception was expected");
         }
         catch (HornetQException e)
         {
            e.printStackTrace();
            assertEquals(HornetQException.ILLEGAL_STATE, e.getCode());
         }

         manager.compareJournals(new JournalLoadInformation[] { new JournalLoadInformation(),
                                                               new JournalLoadInformation() });

         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   // should throw an exception if a second server connects to the same backup
   public void testInvalidConnection() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      FailoverManager failoverManager = createFailoverManager();

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);

         manager.start();

         try
         {
            ReplicationManagerImpl manager2 = new ReplicationManagerImpl(failoverManager,
                                                                         ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);

            manager2.start();
            fail("Exception was expected");
         }
         catch (Exception e)
         {
         }

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

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);

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

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);
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

         assertEquals(1, manager.getActiveTokens().size());

         blockOnReplication(manager);

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
         msg.setBuffer(HornetQChannelBuffers.wrappedBuffer(new byte[10]));

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

         ServerMessageImpl serverMsg = new ServerMessageImpl();
         serverMsg.setMessageID(500);
         serverMsg.setDestination(new SimpleString("tttt"));

         HornetQBuffer buffer = HornetQChannelBuffers.dynamicBuffer(100);
         serverMsg.encodeHeadersAndProperties(buffer);

         manager.largeMessageBegin(500);

         manager.largeMessageWrite(500, new byte[1024]);

         manager.largeMessageDelete(500);

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

   public void testSendPacketsWithFailure() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      ArrayList<String> intercepts = new ArrayList<String>();

      intercepts.add(TestInterceptor.class.getName());

      config.setInterceptorClassNames(intercepts);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);
         manager.start();

         Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

         Thread.sleep(100);
         TestInterceptor.value.set(false);

         for (int i = 0; i < 500; i++)
         {
            replicatedJournal.appendAddRecord(i, (byte)1, new FakeData(), false);
         }

         final CountDownLatch latch = new CountDownLatch(1);
         manager.afterReplicated(new Runnable()
         {
            public void run()
            {
               latch.countDown();
            }
         });

         manager.closeContext();

         server.stop();

         assertTrue(latch.await(50, TimeUnit.SECONDS));
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

      manager.closeContext();

      assertTrue(latch.await(30, TimeUnit.SECONDS));
   }

   public void testNoServer() throws Exception
   {
      FailoverManager failoverManager = createFailoverManager();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);
         manager.start();
         fail("Exception expected");
      }
      catch (HornetQException expected)
      {
         assertEquals(HornetQException.ILLEGAL_STATE, expected.getCode());
      }
   }

   public void testNoActions() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);
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

         assertEquals(1, manager.getActiveTokens().size());

         manager.closeContext();

         assertTrue(latch.await(1, TimeUnit.SECONDS));

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

   public void testOrderOnNonPersistency() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      final ArrayList<Integer> executions = new ArrayList<Integer>();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager,
                                                                     ConfigurationImpl.DEFAULT_BACKUP_WINDOW_SIZE);
         manager.start();

         Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

         int numberOfAdds = 200;
         
         final CountDownLatch latch = new CountDownLatch(numberOfAdds);
         
         for (int i = 0; i < numberOfAdds; i++)
         {
            final int nAdd = i;
            
            if (i % 2 == 0)
            {
               replicatedJournal.appendPrepareRecord(i, new FakeData(), false);
            }
            else
            {
               manager.sync();
            }


            manager.afterReplicated(new Runnable()
            {

               public void run()
               {
                  executions.add(nAdd);
                  latch.countDown();
               }

            });

            manager.closeContext();
         }
         
         assertTrue(latch.await(10, TimeUnit.SECONDS));

         
         for (int i = 0; i < numberOfAdds; i++)
         {
            assertEquals(i, executions.get(i).intValue());
         }
         
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

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      tFactory = new HornetQThreadFactory("HornetQ-ReplicationTest", false);

      executor = Executors.newCachedThreadPool(tFactory);

      scheduledExecutor = new ScheduledThreadPoolExecutor(10, tFactory);

   }

   private FailoverManagerImpl createFailoverManager()
   {
      return createFailoverManager(null);
   }

   private FailoverManagerImpl createFailoverManager(List<Interceptor> interceptors)
   {
      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          new HashMap<String, Object>(),
                                                                          randomString());

      return new FailoverManagerImpl((ClientSessionFactory)null,
                                     connectorConfig,
                                     null,
                                     false,
                                     ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                     ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                     ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                     0,
                                     1.0d,
                                     0,
                                     1,
                                     executor,
                                     scheduledExecutor,
                                     interceptors);
   }

   protected void tearDown() throws Exception
   {

      executor.shutdown();

      scheduledExecutor.shutdown();

      tFactory = null;

      scheduledExecutor = null;

      super.tearDown();

   }

   protected PagingManager createPageManager(StorageManager storageManager,
                                             Configuration configuration,
                                             ExecutorFactory executorFactory,
                                             HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
   {

      PagingManager paging = new PagingManagerImpl(new PagingStoreFactoryNIO(configuration.getPagingDirectory(),
                                                                             executorFactory,
                                                                             false),
                                                   storageManager,
                                                   addressSettingsRepository);

      paging.start();
      return paging;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   public static class TestInterceptor implements Interceptor
   {
      static AtomicBoolean value = new AtomicBoolean(true);

      public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
      {
         return value.get();
      }

   };

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
      public JournalLoadInformation load(LoaderCallback reloadManager) throws Exception
      {

         return new JournalLoadInformation();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#load(java.util.List, java.util.List, org.hornetq.core.journal.TransactionFailureCallback)
       */
      public JournalLoadInformation load(List<RecordInfo> committedRecords,
                                         List<PreparedTransactionInfo> preparedTransactions,
                                         TransactionFailureCallback transactionFailure) throws Exception
      {

         return new JournalLoadInformation();
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

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#loadInternalOnly()
       */
      public JournalLoadInformation loadInternalOnly() throws Exception
      {
         return new JournalLoadInformation();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#getNumberOfRecords()
       */
      public int getNumberOfRecords()
      {
         return 0;
      }

   }
}
