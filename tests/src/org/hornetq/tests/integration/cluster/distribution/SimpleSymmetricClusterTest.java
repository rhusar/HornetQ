/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.distribution;

import java.util.List;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;

/**
 * A SimpleSymmetricClusterTest
 *
 * @author clebert
 *
 *
 */
public class SimpleSymmetricClusterTest extends ClusterTestBase
{

   // Constants -----------------------------------------------------

   static final Logger log = Logger.getLogger(SimpleSymmetricClusterTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   /**
    * @param name
    * @param address
    * @param forwardWhenNoConsumers
    * @param maxHops
    * @param connectorFrom
    * @param pairs
    * @return
    */
   protected ClusterConnectionConfiguration createClusterConfig(final String name,
                                                                final String address,
                                                                final boolean forwardWhenNoConsumers,
                                                                final int maxHops,
                                                                TransportConfiguration connectorFrom,
                                                                List<String> pairs)
   {
      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      connectorFrom.getName(),
                                                                                      2000,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      pairs,
                                                                                      false);
      return clusterConf;
   }

   public void tearDown() throws Exception
   {
      stopServers(0, 1, 2);
      super.tearDown();
   }

   public boolean isNetty()
   {
      return false;
   }

   public void testSimple() throws Exception
   {
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      setupServer(2, true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 2, 0);
      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      Thread.sleep(1000);

      for (int i = 0; i < 10; i++)
         log.info("****************************");
      for (int i = 0; i <= 2; i++)
      {
         log.info("*************************************\n " + servers[i] +
                  " topology:\n" +
                  servers[i].getClusterManager().getTopology().describe());
      }
      for (int i = 0; i < 10; i++)
         log.info("****************************");
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      // Thread.sleep(1500);

      createQueue(0, "queues.testaddress", "queue0", null, false);
      // Thread.sleep(1500);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      // Thread.sleep(1500);
      createQueue(2, "queues.testaddress", "queue0", null, false);
      // Thread.sleep(1500);

      addConsumer(0, 0, "queue0", null);
      // Thread.sleep(1500);
      addConsumer(1, 1, "queue0", null);
      // Thread.sleep(1500);
      addConsumer(2, 2, "queue0", null);
      // Thread.sleep(1500);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

   }
   
   public void testSimpleRoundRobbin() throws Exception
   {
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      setupServer(2, true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, 10, 100, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", false, 1, 10, 100, isNetty(), 1, 2, 0);
      setupClusterConnection("cluster1", "queues", false, 1, 10, 100, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      // Thread.sleep(1500);

      createQueue(0, "queues.testaddress", "queue0", null, true);
      // Thread.sleep(1500);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      // Thread.sleep(1500);
      createQueue(2, "queues.testaddress", "queue0", null, true);
      // Thread.sleep(1500);

      addConsumer(0, 0, "queue0", null);
      // Thread.sleep(1500);
      addConsumer(1, 1, "queue0", null);
      // Thread.sleep(1500);
      addConsumer(2, 2, "queue0", null);
      // Thread.sleep(1500);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 33, true, null);

      verifyReceiveRoundRobin(33, 0, 1, 2);

      stopServers(2);


      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);


      send(0, "queues.testaddress", 100, true, null);

      verifyReceiveRoundRobin(100, 0, 1);
      
      sfs[2] = null;
      consumers[2] = null;
      
      
      startServers(2);
      
      setupSessionFactory(2, isNetty());

      addConsumer(2, 2, "queue0", null);
      
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 33, true, null);

      verifyReceiveRoundRobin(33, 0, 1, 2);

      
      

   }

   
   public void testSimpleRoundRobbinNoFailure() throws Exception
   {
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      setupServer(2, true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, -1, 1000, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", false, 1, -1, 1000, isNetty(), 1, 2, 0);
      setupClusterConnection("cluster1", "queues", false, 1, -1, 1000, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      // Thread.sleep(1500);

      createQueue(0, "queues.testaddress", "queue0", null, true);
      // Thread.sleep(1500);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      // Thread.sleep(1500);
      createQueue(2, "queues.testaddress", "queue0", null, true);
      // Thread.sleep(1500);

      addConsumer(0, 0, "queue0", null);
      // Thread.sleep(1500);
      addConsumer(1, 1, "queue0", null);
      // Thread.sleep(1500);
      addConsumer(2, 2, "queue0", null);
      // Thread.sleep(1500);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 33, true, null);

      verifyReceiveRoundRobin(33, 0, 1, 2);

      Thread.sleep(1000);
      
      // TODO: need to make sure the shutdown won't be send, what will affect the test
      stopServers(2);
//      
//      Thread.sleep(5000);
//
//      waitForBindings(0, "queues.testaddress", 2, 2, false);
//      waitForBindings(1, "queues.testaddress", 2, 2, false);


      send(0, "queues.testaddress", 100, true, null);
      
      verifyReceiveRoundRobin(100, 0, 1, -1);
      
      sfs[2] = null;
      consumers[2] = null;
      
      startServers(2);
      
      setupSessionFactory(2, isNetty());

      addConsumer(2, 2, "queue0", null);
      
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      verifyReceiveRoundRobin(100, -1, -1, 2);

      
      

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
