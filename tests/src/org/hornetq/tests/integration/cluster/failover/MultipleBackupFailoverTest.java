/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.tests.integration.cluster.failover;

import junit.framework.Assert;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.config.BackupConnectorConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.impl.FakeLockFile;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ServiceTestBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class MultipleBackupFailoverTest extends ServiceTestBase
{
   private ArrayList<TestableServer> servers = new ArrayList<TestableServer>(5);

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
      FakeLockFile.clearLocks();
      servers.ensureCapacity(5);
      createConfigs();

      servers.get(1).start();
      servers.get(2).start();
      servers.get(3).start();
      servers.get(4).start();
      servers.get(5).start();
      servers.get(0).start();
   }

   /**
    * @throws Exception
    */
   protected void createConfigs() throws Exception
   {

      createLiveConfig(0);
      createBackupConfig(1, 0, 2, 3, 4, 5);
      createBackupConfig(2, 0, 1, 3, 4, 5);
      createBackupConfig(3, 0, 1, 2, 4, 5);
      createBackupConfig(4, 0, 1, 2, 3, 4);
      createBackupConfig(5, 0, 1, 2, 3, 4);
   }

   public void test() throws Exception
   {
      ServerLocator locator = getServerLocator(0);

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      int backupNode;
      ClientSession session = sendAndConsume(sf, true);
      System.out.println("failing node 0");
      fail(0, session);
      session.close();
      backupNode = waitForBackup(5);
      session = sendAndConsume(sf, false);
      System.out.println("failing node " + backupNode);
      fail(backupNode, session);
      session.close();
      backupNode = waitForBackup(5);
      session = sendAndConsume(sf, false);
      System.out.println("failing node " + backupNode);
      fail(backupNode, session);
      session.close();
      backupNode = waitForBackup(5);
      session = sendAndConsume(sf, false);
      System.out.println("failing node " + backupNode);
      fail(backupNode, session);
      session.close();
      backupNode = waitForBackup(5);
      session = sendAndConsume(sf, false);
      System.out.println("failing node " + backupNode);
      fail(backupNode, session);
      session.close();
      backupNode = waitForBackup(5);
      session = sendAndConsume(sf, false);    
      session.close();
      servers.get(backupNode).stop();
      System.out.println("MultipleBackupFailoverTest.test");
   }

   protected void fail(int node, final ClientSession... sessions) throws Exception
   {
      servers.get(node).crash(sessions);
   }

   protected int waitForBackup(long seconds)
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while(true)
      {
         for (int i = 0, serversSize = servers.size(); i < serversSize; i++)
         {
            TestableServer backupServer = servers.get(i);
            if(backupServer.isInitialised())
            {
               return i;
            }
         }
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
         if(System.currentTimeMillis() > (time + toWait))
         {
            fail("backup server never started");
         }
      }
   }


   private void createBackupConfig(int nodeid, int... nodes)
   {
      Configuration config1 = super.createDefaultConfig();
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(nodeid));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      config1.setClustered(true);
      List<String> staticConnectors = new ArrayList<String>();

      for (int node : nodes)
      {
         TransportConfiguration liveConnector = getConnectorTransportConfiguration(node);
         config1.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
         staticConnectors.add(liveConnector.getName());
      }
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(nodeid);
      List<String> pairs = null;
      ClusterConnectionConfiguration ccc1 = new ClusterConnectionConfiguration("cluster1", "jms", backupConnector.getName(), -1, false, false, 1, 1,
               pairs);
      config1.getClusterConfigurations().add(ccc1);
      BackupConnectorConfiguration connectorConfiguration = new BackupConnectorConfiguration(staticConnectors, backupConnector.getName());
      config1.setBackupConnectorConfiguration(connectorConfiguration);
      config1.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      servers.add(new SameProcessHornetQServer(createFakeLockServer(true, config1)));
   }

   public void createLiveConfig(int liveNode)
   {
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(liveNode);
      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(liveNode));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      config0.setClustered(true);
       List<String> pairs = null;
      ClusterConnectionConfiguration ccc0 = new ClusterConnectionConfiguration("cluster1", "jms", liveConnector.getName(), -1, false, false, 1, 1,
               pairs);
      config0.getClusterConfigurations().add(ccc0);
      config0.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      servers.add(new SameProcessHornetQServer(createFakeLockServer(true, config0)));
   }
   private TransportConfiguration getConnectorTransportConfiguration(int node)
   {
      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put(TransportConstants.SERVER_ID_PROP_NAME, node);
      return new TransportConfiguration(INVM_CONNECTOR_FACTORY, map);
   }

   private TransportConfiguration getAcceptorTransportConfiguration(int node)
   {
      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put(TransportConstants.SERVER_ID_PROP_NAME, node);
      return new TransportConfiguration(INVM_ACCEPTOR_FACTORY, map);
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

   public ServerLocator getServerLocator(int... nodes)
   {
      TransportConfiguration[] configs = new TransportConfiguration[nodes.length];
      for (int i = 0, configsLength = configs.length; i < configsLength; i++)
      {
         HashMap<String, Object> map = new HashMap<String, Object>();
         map.put(TransportConstants.SERVER_ID_PROP_NAME, i);
         configs[i] = new TransportConfiguration(INVM_CONNECTOR_FACTORY, map);

      }
      return new ServerLocatorImpl(true, configs);
   }

   private ClientSession sendAndConsume(final ClientSessionFactory sf, final boolean createQueue) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      if (createQueue)
      {
         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, false);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("aardvarks", message2.getBodyBuffer().readString());

         Assert.assertEquals(i, message2.getObjectProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();

      Assert.assertNull(message3);

      return session;
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
         if (connectorPair.a != null && !liveNode.contains(connectorPair.a.getName()))
         {
            liveNode.add(connectorPair.a.getName());
            latch.countDown();
         }
         if (connectorPair.b != null && !backupNode.contains(connectorPair.b.getName()))
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
