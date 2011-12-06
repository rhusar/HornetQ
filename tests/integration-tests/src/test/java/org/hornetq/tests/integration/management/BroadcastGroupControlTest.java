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
import java.util.List;

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.BroadcastGroupControl;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.BroadcastGroupConstants;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.json.JSONArray;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class BroadcastGroupControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer service;

   // Static --------------------------------------------------------

   public static BroadcastGroupConfiguration randomBroadcastGroupConfiguration(final List<TransportConfiguration> connectors)
   {
      return createBroadcastGroupConfiguration(RandomUtil.randomString(),
                                             null,
                                             1198,
                                             "231.7.7.7",
                                             1199,
                                             RandomUtil.randomPositiveInt(),
                                             connectors);
   }

   public static Pair<String, String> randomPair()
   {
      return new Pair<String, String>(RandomUtil.randomString(), RandomUtil.randomString());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      TransportConfiguration connectorConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
      List<TransportConfiguration> connectorInfos = new ArrayList<TransportConfiguration>();
      connectorInfos.add(connectorConfiguration);
      BroadcastGroupConfiguration broadcastGroupConfig = BroadcastGroupControlTest.randomBroadcastGroupConfiguration(connectorInfos);

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getConnectorConfigurations().put(connectorConfiguration.getName(), connectorConfiguration);
      conf.getBroadcastGroupConfigurations().add(broadcastGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = addServer(HornetQServers.newHornetQServer(conf, mbeanServer, false));
      service.start();

      BroadcastGroupControl broadcastGroupControl = createManagementControl(broadcastGroupConfig.getName());

      Assert.assertEquals(broadcastGroupConfig.getName(), broadcastGroupControl.getName());
      Assert.assertEquals(broadcastGroupConfig.getParams().get(BroadcastGroupConstants.GROUP_ADDRESS_NAME), broadcastGroupControl.getGroupAddress());
      Assert.assertEquals(Integer.parseInt((String)broadcastGroupConfig.getParams().get(BroadcastGroupConstants.GROUP_PORT_NAME)), broadcastGroupControl.getGroupPort());
      Assert.assertEquals(Integer.parseInt((String)broadcastGroupConfig.getParams().get(BroadcastGroupConstants.LOCAL_BIND_PORT_NAME)), broadcastGroupControl.getLocalBindPort());
      Assert.assertEquals(Long.parseLong((String)broadcastGroupConfig.getParams().get(BroadcastGroupConstants.BROADCAST_PERIOD_NAME)), broadcastGroupControl.getBroadcastPeriod());

      Object[] connectorPairs = broadcastGroupControl.getConnectorPairs();
      Assert.assertEquals(1, connectorPairs.length);

      String connectorPairData = (String)connectorPairs[0];
      Assert.assertEquals(broadcastGroupConfig.getConnectorList().get(0).getName(), connectorPairData);
      String jsonString = broadcastGroupControl.getConnectorPairsAsJSON();
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(1, array.length());
      Assert.assertEquals(broadcastGroupConfig.getConnectorList().get(0).getName(), array.getString(0));

      Assert.assertTrue(broadcastGroupControl.isStarted());
   }

   public void testStartStop() throws Exception
   {
      TransportConfiguration connectorConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
      List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();
      connectors.add(connectorConfiguration);
      BroadcastGroupConfiguration broadcastGroupConfig = BroadcastGroupControlTest.randomBroadcastGroupConfiguration(connectors);

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getConnectorConfigurations().put(connectorConfiguration.getName(), connectorConfiguration);
      conf.getBroadcastGroupConfigurations().add(broadcastGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = addServer(HornetQServers.newHornetQServer(conf, mbeanServer, false));
      service.start();

      BroadcastGroupControl broadcastGroupControl = createManagementControl(broadcastGroupConfig.getName());

      // started by the server
      Assert.assertTrue(broadcastGroupControl.isStarted());

      broadcastGroupControl.stop();
      Assert.assertFalse(broadcastGroupControl.isStarted());

      broadcastGroupControl.start();
      Assert.assertTrue(broadcastGroupControl.isStarted());
   }

   protected BroadcastGroupControl createManagementControl(final String name) throws Exception
   {
      return ManagementControlHelper.createBroadcastGroupControl(name, mbeanServer);
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
