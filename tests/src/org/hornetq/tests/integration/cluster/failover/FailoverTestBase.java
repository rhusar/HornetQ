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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.BackupConnectorConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.cluster.impl.FakeLockFile;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A FailoverTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public abstract class FailoverTestBase extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   // Attributes ----------------------------------------------------

   protected TestableServer liveServer;

   protected TestableServer backupServer;

   protected Configuration backupConfig;

   protected Configuration liveConfig;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @param name
    */
   public FailoverTestBase(final String name)
   {
      super(name);
   }

   public FailoverTestBase()
   {
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
      FakeLockFile.clearLocks();
      createConfigs();

      liveServer.start();

      if (backupServer != null)
      {
         backupServer.start();
      }
   }

   protected TestableServer createLiveServer()
   {
      return new SameProcessHornetQServer(createFakeLockServer(true, liveConfig));
   }

   protected TestableServer createBackupServer()
   {
      return new SameProcessHornetQServer(createFakeLockServer(true, backupConfig));
   }

   /**
    * @throws Exception
    */
   protected void createConfigs() throws Exception
   {
      backupConfig = super.createDefaultConfig();
      backupConfig.getAcceptorConfigurations().clear();
      backupConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      backupConfig.setSecurityEnabled(false);
      backupConfig.setSharedStore(true);
      backupConfig.setBackup(true);
      backupConfig.setClustered(true);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      backupConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(liveConnector.getName());
      backupConfig.setBackupConnectorConfiguration(new BackupConnectorConfiguration(staticConnectors, backupConnector.getName()));
      backupServer = createBackupServer();
      
      // FIXME
      /*
      server1Service.registerActivateCallback(new ActivateCallback()
      {
         
         public void preActivate()
         {
            // To avoid two servers messing up with the same journal at any single point

         }
         
         public void activated()
         {
            try
            {
               liveServer.getStorageManager().stop();
            }
            catch (Exception ignored)
            {
            }
         }
      });
*/
      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(true);
      liveConfig.setClustered(true);
       List<String> pairs = null;
      ClusterConnectionConfiguration ccc0 = new ClusterConnectionConfiguration("cluster1", "jms", liveConnector.getName(), -1, false, false, 1, 1,
               pairs);
      liveConfig.getClusterConfigurations().add(ccc0);
      liveConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      liveServer = createLiveServer();
   }

   protected void createReplicatedConfigs() throws Exception
   {
      Configuration config1 = super.createDefaultConfig();
      config1.setBindingsDirectory(config1.getBindingsDirectory() + "_backup");
      config1.setJournalDirectory(config1.getJournalDirectory() + "_backup");
      config1.setPagingDirectory(config1.getPagingDirectory() + "_backup");
      config1.setLargeMessagesDirectory(config1.getLargeMessagesDirectory() + "_backup");
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(false);
      config1.setBackup(true);
      backupServer = createBackupServer();
      
      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));

      config0.getConnectorConfigurations().put("toBackup", getConnectorTransportConfiguration(false));
      //liveConfig.setBackupConnectorName("toBackup");
      config0.setSecurityEnabled(false);
      config0.setSharedStore(false);
      liveServer = createLiveServer();

      backupServer.start();
      liveServer.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupServer.stop();

      liveServer.stop();

      Assert.assertEquals(0, InVMRegistry.instance.size());

      backupServer = null;

      liveServer = null;

      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
   }

   protected ClientSessionFactoryInternal createSessionFactoryAndWaitForTopology(ServerLocator locator, int topologyMembers)
           throws Exception
     {
        ClientSessionFactoryInternal sf;
        CountDownLatch countDownLatch = new CountDownLatch(topologyMembers);

        locator.addClusterTopologyListener(new LatchClusterTopologyListener(countDownLatch));

        sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        return sf;
     }

   protected void waitForBackup(long seconds)
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while(!backupServer.isInitialised())
      {
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
         if(backupServer.isInitialised())
         {
            break;
         }
         else if(System.currentTimeMillis() > (time + toWait))
         {
            fail("backup server never started");
         }
      }
      System.out.println("FailoverTestBase.waitForBackup");
   }

   protected TransportConfiguration getInVMConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory", server1Params);
      }
   }

   protected TransportConfiguration getInVMTransportAcceptorConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", server1Params);
      }
   }

   protected TransportConfiguration getNettyAcceptorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                           org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory",
                                           server1Params);
      }
   }

   protected TransportConfiguration getNettyConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                           org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory",
                                           server1Params);
      }
   }

   protected abstract TransportConfiguration getAcceptorTransportConfiguration(boolean live);

   protected abstract TransportConfiguration getConnectorTransportConfiguration(final boolean live);

   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(true));// , getConnectorTransportConfiguration(false));
      return (ServerLocatorInternal) locator;
   }

   protected void crash(final ClientSession... sessions) throws Exception
   {
      liveServer.crash(sessions);
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   abstract class BaseListener implements SessionFailureListener
   {
      public void beforeReconnect(final HornetQException me)
      {
      }
   }
   
   class LatchClusterTopologyListener implements ClusterTopologyListener
   {
      final CountDownLatch latch;
      int liveNodes = 0;
      int backUpNodes = 0;
      List<String> liveNode = new ArrayList<String>();
      List<String> backupNode = new ArrayList<String>();

      public LatchClusterTopologyListener(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void nodeUP(String nodeID, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last, int distance)
      {
         if(connectorPair.a != null && !liveNode.contains(connectorPair.a.getName()))
         {
            liveNode.add(connectorPair.a.getName());
            latch.countDown();
         }
         if(connectorPair.b != null && !backupNode.contains(connectorPair.b.getName()))
         {
            backupNode.add(connectorPair.b.getName());
            latch.countDown();
         }
      }

      public void nodeDown(String nodeID)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }
   }
}
