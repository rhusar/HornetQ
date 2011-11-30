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

package org.hornetq.tests.integration.discovery;

 import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.management.Notification;
import org.hornetq.integration.discovery.jgroups.BroadcastGroupConstants;
import org.hornetq.integration.discovery.jgroups.DiscoveryGroupConstants;
import org.hornetq.integration.discovery.jgroups.JGroupsBroadcastGroupImpl;
import org.hornetq.integration.discovery.jgroups.JGroupsDiscoveryGroupImpl;
import org.hornetq.tests.integration.SimpleNotificationService;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUIDGenerator;

/**
 * A DiscoveryTest
 *
 * @author <a href="mailto:tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 * 
 */
public class JGroupsDiscoveryTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(DiscoveryTest.class);

   private static final String channelName = DiscoveryGroupConstants.DEFAULT_JGROUPS_CHANNEL_NAME;

   private static final String channelName2 = DiscoveryGroupConstants.DEFAULT_JGROUPS_CHANNEL_NAME + "2";

   private static final String channelName3 = DiscoveryGroupConstants.DEFAULT_JGROUPS_CHANNEL_NAME + "3";

   private static final String config = "test-jgroups-file_ping.xml";

   private static final String config2 = "test-jgroups-file_ping_2.xml";

   private static final String config3 = "test-jgroups-file_ping_3.xml";

   public void testSimpleBroadcast() throws Exception
   {
      final long timeout = 500;

      final String nodeID = RandomUtil.randomString();

      Map<String,Object> params = new HashMap<String,Object>();
      params.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      params.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName);
      TransportConfiguration live1 = generateTC();
      List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();
      connectors.add(live1);
      BroadcastGroupConfiguration broadcastConf = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors);
      BroadcastGroup bg = new JGroupsBroadcastGroupImpl(nodeID, broadcastConf.getName(), true, broadcastConf);

      bg.start();

      DiscoveryGroup dg = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                        RandomUtil.randomString(),
                                                        channelName,
                                                        Thread.currentThread().getContextClassLoader().getResource(config),
                                                        timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(10000);

      Assert.assertTrue(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      bg.stop();

      dg.stop();

   }
   
   public void testSimpleBroadcastWithStopStartDiscoveryGroup() throws Exception
   {
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      Map<String,Object> params = new HashMap<String,Object>();
      params.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      params.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName);
      TransportConfiguration live1 = generateTC();
      List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();
      connectors.add(live1);
      BroadcastGroupConfiguration broadcastConf = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors);
      BroadcastGroup bg = new JGroupsBroadcastGroupImpl(nodeID, broadcastConf.getName(), true, broadcastConf);

      bg.start();

      DiscoveryGroup dg = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 channelName,
                                                 Thread.currentThread().getContextClassLoader().getResource(config),
                                                 timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      bg.stop();

      dg.stop();

      dg.start();

      bg.start();

      bg.broadcastConnectors();

      ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
      
      bg.stop();
      
      dg.stop();
   }

   public void testIgnoreTrafficFromOwnNode() throws Exception
   {
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      Map<String,Object> params = new HashMap<String,Object>();
      params.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      params.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName);
      TransportConfiguration live1 = generateTC();
      List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();
      connectors.add(live1);
      BroadcastGroupConfiguration broadcastConf = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors);
      BroadcastGroup bg = new JGroupsBroadcastGroupImpl(nodeID, broadcastConf.getName(), true, broadcastConf);

      bg.start();

      DiscoveryGroup dg = new JGroupsDiscoveryGroupImpl(nodeID,
                                                        RandomUtil.randomString(),
                                                        channelName,
                                                        Thread.currentThread().getContextClassLoader().getResource(config),
                                                        timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertFalse(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();

      Assert.assertNotNull(entries);

      Assert.assertEquals(0, entries.size());

      bg.stop();

      dg.stop();

   }


   public void testMultipleGroups() throws Exception
   {
      final int timeout = 500;

      String node1 = RandomUtil.randomString();

      String node2 = RandomUtil.randomString();

      String node3 = RandomUtil.randomString();

      Map<String,Object> params1 = new HashMap<String,Object>();
      params1.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      params1.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName);
      TransportConfiguration live1 = generateTC();
      List<TransportConfiguration> connectors1 = new ArrayList<TransportConfiguration>();
      connectors1.add(live1);
      BroadcastGroupConfiguration broadcastConf1 = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params1, connectors1);
      BroadcastGroup bg1 = new JGroupsBroadcastGroupImpl(node1, broadcastConf1.getName(), true, broadcastConf1);
      bg1.start();

      Map<String,Object> params2 = new HashMap<String,Object>();
      params2.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config2);
      params2.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName2);
      TransportConfiguration live2 = generateTC();
      List<TransportConfiguration> connectors2 = new ArrayList<TransportConfiguration>();
      connectors2.add(live2);
      BroadcastGroupConfiguration broadcastConf2 = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params2, connectors2);
      BroadcastGroup bg2 = new JGroupsBroadcastGroupImpl(node2, broadcastConf2.getName(), true, broadcastConf2);
      bg2.start();

      Map<String,Object> params3 = new HashMap<String,Object>();
      params3.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config3);
      params3.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName3);
      TransportConfiguration live3 = generateTC();
      List<TransportConfiguration> connectors3 = new ArrayList<TransportConfiguration>();
      connectors3.add(live3);
      BroadcastGroupConfiguration broadcastConf3 = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params3, connectors3);
      BroadcastGroup bg3 = new JGroupsBroadcastGroupImpl(node3, broadcastConf3.getName(), true, broadcastConf3);
      bg3.start();

      DiscoveryGroup dg1 = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  channelName,
                                                  Thread.currentThread().getContextClassLoader().getResource(config),
                                                  timeout);
      dg1.start();

      DiscoveryGroup dg2 = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  channelName2,
                                                  Thread.currentThread().getContextClassLoader().getResource(config2),
                                                  timeout);
      dg2.start();

      DiscoveryGroup dg3 = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  channelName3,
                                                  Thread.currentThread().getContextClassLoader().getResource(config3),
                                                  timeout);
      dg3.start();

      bg1.broadcastConnectors();

      bg2.broadcastConnectors();

      bg3.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      List<DiscoveryEntry> entries = dg1.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg2.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg2.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live2), entries);

      ok = dg3.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg3.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live3), entries);

      bg1.stop();
      bg2.stop();
      bg3.stop();

      dg1.stop();
      dg2.stop();
      dg3.stop();
   }

   public void testDiscoveryListenersCalled() throws Exception
   {
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      Map<String,Object> params = new HashMap<String,Object>();
      params.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      params.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName);
      TransportConfiguration live1 = generateTC();
      List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();
      connectors.add(live1);
      BroadcastGroupConfiguration broadcastConf = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors);
      BroadcastGroup bg = new JGroupsBroadcastGroupImpl(nodeID, broadcastConf.getName(), true, broadcastConf);

      bg.start();

      DiscoveryGroup dg = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                        RandomUtil.randomString(),
                                                        channelName,
                                                        Thread.currentThread().getContextClassLoader().getResource(config),
                                                        timeout);

      MyListener listener1 = new MyListener();
      MyListener listener2 = new MyListener();
      MyListener listener3 = new MyListener();

      dg.registerListener(listener1);
      dg.registerListener(listener2);
      dg.registerListener(listener3);

      dg.start();

      bg.broadcastConnectors();
      boolean ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);

      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      Assert.assertTrue(listener3.called);

      listener1.called = false;
      listener2.called = false;
      listener3.called = false;

      bg.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);

      // Won't be called since connectors haven't changed
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      Assert.assertFalse(listener3.called);

      bg.stop();

      dg.stop();
   }

   public void testConnectorsUpdatedMultipleBroadcasters() throws Exception
   {
      final int timeout = 500;

      String node1 = RandomUtil.randomString();
      String node2 = RandomUtil.randomString();
      String node3 = RandomUtil.randomString();

      Map<String,Object> params = new HashMap<String,Object>();
      params.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      params.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName);

      TransportConfiguration live1 = generateTC();
      List<TransportConfiguration> connectors1 = new ArrayList<TransportConfiguration>();
      connectors1.add(live1);
      BroadcastGroupConfiguration broadcastConf1 = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors1);
      BroadcastGroup bg1 = new JGroupsBroadcastGroupImpl(node1, broadcastConf1.getName(), true, broadcastConf1);
      bg1.start();

      TransportConfiguration live2 = generateTC();
      List<TransportConfiguration> connectors2 = new ArrayList<TransportConfiguration>();
      connectors2.add(live2);
      BroadcastGroupConfiguration broadcastConf2 = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors2);
      BroadcastGroup bg2 = new JGroupsBroadcastGroupImpl(node2, broadcastConf2.getName(), true, broadcastConf2);
      bg2.start();

      TransportConfiguration live3 = generateTC();
      List<TransportConfiguration> connectors3 = new ArrayList<TransportConfiguration>();
      connectors3.add(live3);
      BroadcastGroupConfiguration broadcastConf3 = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors3);
      BroadcastGroup bg3 = new JGroupsBroadcastGroupImpl(node3, broadcastConf3.getName(), true, broadcastConf3);
      bg3.start();

      DiscoveryGroup dg = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                        RandomUtil.randomString(),
                                                        channelName,
                                                        Thread.currentThread().getContextClassLoader().getResource(config),
                                                        timeout);

      MyListener listener1 = new MyListener();
      dg.registerListener(listener1);
      MyListener listener2 = new MyListener();
      dg.registerListener(listener2);

      dg.start();

      bg1.broadcastConnectors();
      boolean ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.removeConnector(live2);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);

      // Connector2 should still be there since not timed out yet

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      Thread.sleep(timeout);

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live3), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.removeConnector(live1);
      bg3.removeConnector(live3);

      Thread.sleep(timeout);

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      Assert.assertNotNull(entries);
      Assert.assertEquals(0, entries.size());
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      Assert.assertNotNull(entries);
      Assert.assertEquals(0, entries.size());
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);

      bg1.stop();
      bg2.stop();
      bg3.stop();

      dg.stop();
   }

   public void testMultipleDiscoveryGroups() throws Exception
   {
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      Map<String,Object> params = new HashMap<String,Object>();
      params.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      params.put(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, channelName);
      TransportConfiguration live1 = generateTC();
      List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();
      connectors.add(live1);
      BroadcastGroupConfiguration broadcastConf = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, connectors);
      BroadcastGroup bg = new JGroupsBroadcastGroupImpl(nodeID, broadcastConf.getName(), true, broadcastConf);
      bg.start();

      DiscoveryGroup dg1 = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                         RandomUtil.randomString(),
                                                         channelName,
                                                         Thread.currentThread().getContextClassLoader().getResource(config),
                                                         timeout);

      DiscoveryGroup dg2 = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                         RandomUtil.randomString(),
                                                         channelName,
                                                         Thread.currentThread().getContextClassLoader().getResource(config),
                                                         timeout);

      DiscoveryGroup dg3 = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                         RandomUtil.randomString(),
                                                         channelName,
                                                         Thread.currentThread().getContextClassLoader().getResource(config),
                                                         timeout);

      dg1.start();
      dg2.start();
      dg3.start();

      bg.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      List<DiscoveryEntry> entries = dg1.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg2.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg2.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg3.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg3.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      bg.stop();

      dg1.stop();
      dg2.stop();
      dg3.stop();
   }

   public void testDiscoveryGroupNotifications() throws Exception
   {
      SimpleNotificationService notifService = new SimpleNotificationService();
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      notifService.addNotificationListener(notifListener);

      final int timeout = 500;

      DiscoveryGroup dg = new JGroupsDiscoveryGroupImpl(RandomUtil.randomString(),
                                                        RandomUtil.randomString(),
                                                        channelName,
                                                        Thread.currentThread().getContextClassLoader().getResource(config),
                                                        timeout);
      dg.setNotificationService(notifService);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      dg.start();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(NotificationType.DISCOVERY_GROUP_STARTED, notif.getType());
      Assert.assertEquals(dg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());

      dg.stop();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(NotificationType.DISCOVERY_GROUP_STOPPED, notif.getType());
      Assert.assertEquals(dg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());
   }

   public void testBroadcastGroupNotifications() throws Exception
   {
      SimpleNotificationService notifService = new SimpleNotificationService();
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      notifService.addNotificationListener(notifListener);

      Map<String,Object> params = new HashMap<String,Object>();
      params.put(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, config);
      BroadcastGroupConfiguration broadcastConf = new BroadcastGroupConfiguration(RandomUtil.randomString(), JGroupsBroadcastGroupImpl.class.getName(), params, new ArrayList<TransportConfiguration>());
      BroadcastGroup bg = new JGroupsBroadcastGroupImpl(RandomUtil.randomString(), broadcastConf.getName(), true, broadcastConf);
      bg.setNotificationService(notifService);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      bg.start();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(NotificationType.BROADCAST_GROUP_STARTED, notif.getType());
      Assert.assertEquals(bg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());

      bg.stop();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(NotificationType.BROADCAST_GROUP_STOPPED, notif.getType());
      Assert.assertEquals(bg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());
   }

   private TransportConfiguration generateTC()
   {
      String className = "org.foo.bar." + UUIDGenerator.getInstance().generateStringUUID();
      String name = UUIDGenerator.getInstance().generateStringUUID();
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(UUIDGenerator.getInstance().generateStringUUID(), 123);
      params.put(UUIDGenerator.getInstance().generateStringUUID(), UUIDGenerator.getInstance().generateStringUUID());
      params.put(UUIDGenerator.getInstance().generateStringUUID(), true);
      TransportConfiguration tc = new TransportConfiguration(className, params, name);
      return tc;
   }

   private static class MyListener implements DiscoveryListener
   {
      volatile boolean called;

      public void connectorsChanged()
      {
         called = true;
      }
   }
   
   
   private static void assertEqualsDiscoveryEntries(List<TransportConfiguration> expected, List<DiscoveryEntry> actual)
   {
      assertNotNull(actual);
      
      List<TransportConfiguration> sortedExpected = new ArrayList<TransportConfiguration>(expected);
      Collections.sort(sortedExpected, new Comparator<TransportConfiguration>()
      {

         public int compare(TransportConfiguration o1, TransportConfiguration o2)
         {
            return o2.toString().compareTo(o1.toString());
         }
      });
      List<DiscoveryEntry> sortedActual = new ArrayList<DiscoveryEntry>(actual);
      Collections.sort(sortedActual, new Comparator<DiscoveryEntry>()
      {
         public int compare(DiscoveryEntry o1, DiscoveryEntry o2)
         {
            return o2.getConnector().toString().compareTo(o1.getConnector().toString());
         }
      });
      
      assertEquals(sortedExpected.size(), sortedActual.size());
      for (int i = 0; i < sortedExpected.size(); i++)
      {
         assertEquals(sortedExpected.get(i), sortedActual.get(i).getConnector());
      }
   }

}
