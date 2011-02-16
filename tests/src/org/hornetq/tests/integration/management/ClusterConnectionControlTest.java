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

package org.hornetq.tests.integration.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import junit.framework.Assert;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ClusterConnectionControl;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.management.Notification;
import org.hornetq.tests.integration.SimpleNotificationService;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.json.JSONArray;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 */
public class ClusterConnectionControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server_0;

   private ClusterConnectionConfiguration clusterConnectionConfig1;

   private ClusterConnectionConfiguration clusterConnectionConfig2;

   private HornetQServer server_1;

   private MBeanServer mbeanServer_1;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes1() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(clusterConnectionConfig1.getName()));

      ClusterConnectionControl clusterConnectionControl = createManagementControl(clusterConnectionConfig1.getName());

      Assert.assertEquals(clusterConnectionConfig1.getName(), clusterConnectionControl.getName());
      Assert.assertEquals(clusterConnectionConfig1.getAddress(), clusterConnectionControl.getAddress());
      Assert.assertEquals(clusterConnectionConfig1.getDiscoveryGroupConfiguration().getName(),
                          clusterConnectionControl.getDiscoveryGroupName());
      Assert.assertEquals(clusterConnectionConfig1.getRetryInterval(), clusterConnectionControl.getRetryInterval());
      Assert.assertEquals(clusterConnectionConfig1.isDuplicateDetection(),
                          clusterConnectionControl.isDuplicateDetection());
      Assert.assertEquals(clusterConnectionConfig1.isForwardWhenNoConsumers(),
                          clusterConnectionControl.isForwardWhenNoConsumers());
      Assert.assertEquals(clusterConnectionConfig1.getMaxHops(), clusterConnectionControl.getMaxHops());

      Object[] connectors = clusterConnectionControl.getStaticConnectors();
      Assert.assertEquals(1, connectors.length);
      String connector = (String)connectors[0];
      TransportConfiguration[] clusterConnectors = (TransportConfiguration[])clusterConnectionConfig1.getDiscoveryGroupConfiguration().getParams().get(DiscoveryGroupConstants.STATIC_CONNECTORS_LIST_NAME);
      Assert.assertEquals(clusterConnectors[0].getName(), connector);

      String jsonString = clusterConnectionControl.getStaticConnectorsAsJSON();
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(1, array.length());
      Assert.assertEquals(clusterConnectors[0].getName(), array.getString(0));
      
      Assert.assertNull(clusterConnectionControl.getDiscoveryGroupName());

      Assert.assertTrue(clusterConnectionControl.isStarted());
   }

   public void testAttributes2() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(clusterConnectionConfig2.getName()));

      ClusterConnectionControl clusterConnectionControl = createManagementControl(clusterConnectionConfig2.getName());

      Assert.assertEquals(clusterConnectionConfig2.getName(), clusterConnectionControl.getName());
      Assert.assertEquals(clusterConnectionConfig2.getAddress(), clusterConnectionControl.getAddress());
      Assert.assertEquals(clusterConnectionConfig2.getDiscoveryGroupConfiguration().getName(),
                          clusterConnectionControl.getDiscoveryGroupName());
      Assert.assertEquals(clusterConnectionConfig2.getRetryInterval(), clusterConnectionControl.getRetryInterval());
      Assert.assertEquals(clusterConnectionConfig2.isDuplicateDetection(),
                          clusterConnectionControl.isDuplicateDetection());
      Assert.assertEquals(clusterConnectionConfig2.isForwardWhenNoConsumers(),
                          clusterConnectionControl.isForwardWhenNoConsumers());
      Assert.assertEquals(clusterConnectionConfig2.getMaxHops(), clusterConnectionControl.getMaxHops());

      Object[] connectorPairs = clusterConnectionControl.getStaticConnectors();
      Assert.assertNull(connectorPairs);

      String jsonPairs = clusterConnectionControl.getStaticConnectorsAsJSON();
      Assert.assertNull(jsonPairs);

      Assert.assertEquals(clusterConnectionConfig2.getDiscoveryGroupConfiguration().getName(),
                          clusterConnectionControl.getDiscoveryGroupName());
   }

   public void testStartStop() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(clusterConnectionConfig1.getName()));
      ClusterConnectionControl clusterConnectionControl = createManagementControl(clusterConnectionConfig1.getName());

      // started by the server
      Assert.assertTrue(clusterConnectionControl.isStarted());

      clusterConnectionControl.stop();
      Assert.assertFalse(clusterConnectionControl.isStarted());

      clusterConnectionControl.start();
      Assert.assertTrue(clusterConnectionControl.isStarted());
   }

   public void testNotifications() throws Exception
   {
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      checkResource(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(clusterConnectionConfig1.getName()));
      ClusterConnectionControl clusterConnectionControl = createManagementControl(clusterConnectionConfig1.getName());

      server_0.getManagementService().addNotificationListener(notifListener);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      clusterConnectionControl.stop();

      Assert.assertTrue(notifListener.getNotifications().size() > 0);
      Notification notif = notifListener.getNotifications().get(notifListener.getNotifications().size() - 1);
      Assert.assertEquals(NotificationType.CLUSTER_CONNECTION_STOPPED, notif.getType());
      Assert.assertEquals(clusterConnectionControl.getName(), notif.getProperties()
                                                                   .getSimpleStringProperty(new SimpleString("name"))
                                                                   .toString());

      clusterConnectionControl.start();

      Assert.assertTrue(notifListener.getNotifications().size() > 0);
      notif = notifListener.getNotifications().get(notifListener.getNotifications().size() - 1);
      Assert.assertEquals(NotificationType.CLUSTER_CONNECTION_STARTED, notif.getType());
      Assert.assertEquals(clusterConnectionControl.getName(), notif.getProperties()
                                                                   .getSimpleStringProperty(new SimpleString("name"))
                                                                   .toString());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> acceptorParams = new HashMap<String, Object>();
      acceptorParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         acceptorParams,
                                                                         RandomUtil.randomString());

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          acceptorParams,
                                                                          RandomUtil.randomString());

      CoreQueueConfiguration queueConfig = new CoreQueueConfiguration(RandomUtil.randomString(),
                                                              RandomUtil.randomString(),
                                                              null,
                                                              false);
      DiscoveryGroupConfiguration staticGroupConfig = createStaticDiscoveryGroupConfiguration(connectorConfig);
      
      clusterConnectionConfig1 = new ClusterConnectionConfiguration(RandomUtil.randomString(),
                                                                    queueConfig.getAddress(),
                                                                    connectorConfig.getName(),
                                                                    RandomUtil.randomPositiveLong(),
                                                                    RandomUtil.randomBoolean(),
                                                                    RandomUtil.randomBoolean(),
                                                                    RandomUtil.randomPositiveInt(),
                                                                    RandomUtil.randomPositiveInt(),
                                                                    staticGroupConfig, false);

      String discoveryGroupName = RandomUtil.randomString();
      DiscoveryGroupConfiguration discoveryGroupConfig = createSimpleUDPDiscoveryGroupConfiguration(discoveryGroupName, null, "230.1.2.3", 6745, 500, 0);

      clusterConnectionConfig2 = new ClusterConnectionConfiguration(RandomUtil.randomString(),
                                                                    queueConfig.getAddress(),
                                                                    connectorConfig.getName(),
                                                                    RandomUtil.randomPositiveLong(),
                                                                    RandomUtil.randomBoolean(),
                                                                    RandomUtil.randomBoolean(),
                                                                    RandomUtil.randomPositiveInt(),
                                                                    RandomUtil.randomPositiveInt(),
                                                                    discoveryGroupConfig, false);
      
      Configuration conf_1 = createBasicConfig();
      conf_1.setSecurityEnabled(false);
      conf_1.setJMXManagementEnabled(true);
      conf_1.setClustered(true);
      conf_1.getAcceptorConfigurations().add(acceptorConfig);
      conf_1.getQueueConfigurations().add(queueConfig);

      Configuration conf_0 = createBasicConfig();
      conf_0.setSecurityEnabled(false);
      conf_0.setJMXManagementEnabled(true);
      conf_0.setClustered(true);
      conf_0.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      conf_0.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      conf_0.getClusterConfigurations().add(clusterConnectionConfig1);
      conf_0.getClusterConfigurations().add(clusterConnectionConfig2);
      conf_0.getDiscoveryGroupConfigurations().put(staticGroupConfig.getName(), staticGroupConfig);
      conf_0.getDiscoveryGroupConfigurations().put(discoveryGroupName, discoveryGroupConfig);

      mbeanServer_1 = MBeanServerFactory.createMBeanServer();
      server_1 = HornetQServers.newHornetQServer(conf_1, mbeanServer_1, false);
      server_1.start();

      server_0 = HornetQServers.newHornetQServer(conf_0, mbeanServer, false);
      server_0.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server_0.stop();
      server_1.stop();

      server_0 = null;

      server_1 = null;

      MBeanServerFactory.releaseMBeanServer(mbeanServer_1);
      mbeanServer_1 = null;

      super.tearDown();
   }

   protected ClusterConnectionControl createManagementControl(final String name) throws Exception
   {
      return ManagementControlHelper.createClusterConnectionControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
