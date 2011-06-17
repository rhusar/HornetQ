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

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void setUp() throws Exception 
   {
      super.setUp();
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      
      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0);
 
      startServers(1, 0);
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
                                                                                      pairs, false);
      return clusterConf;
   }


   public void tearDown() throws Exception
   {
      stopServers(0, 1);
      super.tearDown();
   }
   
   public boolean isNetty()
   {
      return false;
   }
   
   public void testSimple() throws Exception
   {
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
