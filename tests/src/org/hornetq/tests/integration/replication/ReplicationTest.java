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
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import javax.management.MBeanServer;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ConnectionManager;
import org.hornetq.core.client.impl.ConnectionManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.replication.impl.ReplicationEndpointImpl;
import org.hornetq.core.replication.impl.ReplicationManagerImpl;
import org.hornetq.core.security.HornetQSecurityManager;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.version.Version;
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
         ReplicationManagerImpl manager = new ReplicationManagerImpl(connectionManager);
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
         ReplicationManagerImpl manager = new ReplicationManagerImpl(connectionManager);
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
         ReplicationManagerImpl manager = new ReplicationManagerImpl(connectionManager);
         manager.start();
         manager.appendAddRecord(1, (byte)1, new DataImplement());
         Thread.sleep(1000);
         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }
   
   class DataImplement implements EncodingSupport
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

   static class FakeServer implements HornetQServer
   {

      public Queue createQueue(SimpleString address,
                               SimpleString queueName,
                               SimpleString filter,
                               boolean durable,
                               boolean temporary) throws Exception
      {
         return null;
      }

      public CreateSessionResponseMessage createSession(String name,
                                                        long channelID,
                                                        String username,
                                                        String password,
                                                        int minLargeMessageSize,
                                                        int incrementingVersion,
                                                        RemotingConnection remotingConnection,
                                                        boolean autoCommitSends,
                                                        boolean autoCommitAcks,
                                                        boolean preAcknowledge,
                                                        boolean xa,
                                                        int producerWindowSize) throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#deployQueue(org.hornetq.utils.SimpleString, org.hornetq.utils.SimpleString, org.hornetq.utils.SimpleString, boolean, boolean)
       */
      public Queue deployQueue(SimpleString address,
                               SimpleString queueName,
                               SimpleString filterString,
                               boolean durable,
                               boolean temporary) throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#destroyQueue(org.hornetq.utils.SimpleString, org.hornetq.core.server.ServerSession)
       */
      public void destroyQueue(SimpleString queueName, ServerSession session) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getAddressSettingsRepository()
       */
      public HierarchicalRepository<AddressSettings> getAddressSettingsRepository()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getClusterManager()
       */
      public ClusterManager getClusterManager()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getConfiguration()
       */
      public Configuration getConfiguration()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getConnectionCount()
       */
      public int getConnectionCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getExecutorFactory()
       */
      public ExecutorFactory getExecutorFactory()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getHornetQServerControl()
       */
      public HornetQServerControlImpl getHornetQServerControl()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getMBeanServer()
       */
      public MBeanServer getMBeanServer()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getManagementService()
       */
      public ManagementService getManagementService()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getNodeID()
       */
      public SimpleString getNodeID()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getPostOffice()
       */
      public PostOffice getPostOffice()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getQueueFactory()
       */
      public QueueFactory getQueueFactory()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getRemotingService()
       */
      public RemotingService getRemotingService()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getResourceManager()
       */
      public ResourceManager getResourceManager()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getSecurityManager()
       */
      public HornetQSecurityManager getSecurityManager()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getSecurityRepository()
       */
      public HierarchicalRepository<Set<Role>> getSecurityRepository()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getSession(java.lang.String)
       */
      public ServerSession getSession(String name)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getSessions()
       */
      public Set<ServerSession> getSessions()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getSessions(java.lang.String)
       */
      public List<ServerSession> getSessions(String connectionID)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getStorageManager()
       */
      public StorageManager getStorageManager()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#getVersion()
       */
      public Version getVersion()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#isInitialised()
       */
      public boolean isInitialised()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#isStarted()
       */
      public boolean isStarted()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#reattachSession(org.hornetq.core.remoting.RemotingConnection, java.lang.String, int)
       */
      public ReattachSessionResponseMessage reattachSession(RemotingConnection connection,
                                                            String name,
                                                            int lastReceivedCommandID) throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#registerActivateCallback(org.hornetq.core.server.ActivateCallback)
       */
      public void registerActivateCallback(ActivateCallback callback)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#removeSession(java.lang.String)
       */
      public void removeSession(String name) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQServer#unregisterActivateCallback(org.hornetq.core.server.ActivateCallback)
       */
      public void unregisterActivateCallback(ActivateCallback callback)
      {

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
       * @see org.hornetq.core.server.HornetQServer#createReplicationEndpoint()
       */
      public ReplicationEndpoint createReplicationEndpoint()
      {
         return new ReplicationEndpointImpl(this);
      }

   }
}
