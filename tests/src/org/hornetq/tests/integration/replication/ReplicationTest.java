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

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ConnectionManager;
import org.hornetq.core.client.impl.ConnectionManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.replication.impl.ReplicationManagerImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;

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
         
         manager.appendAddRecord((byte)0, 1, (byte)1, new FakeData());
         manager.appendUpdateRecord((byte)0, 1, (byte)2, new FakeData());
         manager.appendDeleteRecord((byte)0, 1);
         manager.appendAddRecordTransactional((byte)0, 2, 2, (byte)1, new FakeData());
         manager.appendUpdateRecordTransactional((byte)0, 2, 2, (byte)2, new FakeData());
         manager.appendCommitRecord((byte)0, 2);
         
         manager.appendDeleteRecordTransactional((byte)0, 3, 4,new FakeData());
         manager.appendPrepareRecord((byte)0, 3, new FakeData());
         manager.appendRollbackRecord((byte)0, 3);

         final CountDownLatch latch = new CountDownLatch(1);
         manager.getReplicationToken().addFutureCompletion(new Runnable()
         {

            public void run()
            {
               latch.countDown();
            }

         });
         assertTrue(latch.await(1, TimeUnit.SECONDS));
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
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
