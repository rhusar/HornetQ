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
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.NullResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.remoting.server.HandlerFactory;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.security.HornetQSecurityManager;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.impl.HornetQPacketHandler;
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

   private RemotingService remoting;

   private ThreadFactory tFactory;

   private ExecutorService executor;

   private ConnectionManager connectionManagerLive;

   private ScheduledExecutorService scheduledExecutor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testBasicConnection() throws Exception
   {

      RemotingConnection conn = connectionManagerLive.getConnection(1);

      Channel chann = conn.getChannel(2, -1, false);

      chann.close();

   }

   // Package protected ---------------------------------------------

   class LocalHandler implements ChannelHandler
   {

      final Channel channel;

      /**
       * @param channel
       */
      public LocalHandler(Channel channel)
      {
         super();
         this.channel = channel;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.remoting.ChannelHandler#handlePacket(org.hornetq.core.remoting.Packet)
       */
      public void handlePacket(Packet packet)
      {
         channel.send(new NullResponseMessage());
      }

   }

   HandlerFactory handlerFactory = new HandlerFactory()
   {

      public ChannelHandler getHandler(RemotingConnection conn, Channel channel)
      {
         System.out.println("Created a handler");
         return new LocalHandler(channel);
      }

   };

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      Configuration config = createDefaultConfig(false);

      tFactory = new HornetQThreadFactory("HornetQ-ReplicationTest", false);

      executor = Executors.newCachedThreadPool(tFactory);

      scheduledExecutor = new ScheduledThreadPoolExecutor(10, tFactory);

      remoting = new RemotingServiceImpl(config, handlerFactory, null, null, executor, scheduledExecutor, 0);

      remoting.start();

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          new HashMap<String, Object>(),
                                                                          randomString());

      List<Interceptor> interceptors = new ArrayList<Interceptor>();

      connectionManagerLive = new ConnectionManagerImpl(null,
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
      remoting.stop();

      executor.shutdown();

      scheduledExecutor.shutdown();

      remoting = null;

      tFactory = null;

      scheduledExecutor = null;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
