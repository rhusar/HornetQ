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
package org.hornetq.tests.integration.cluster.distribution;

import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.server.group.impl.Response;
import org.hornetq.core.server.group.impl.Proposal;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.management.Notification;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClusteredGroupingTest extends ClusterTestBase
{

   public void testGroupingSimple() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);

         /*waitForBindings(0, "queues.testaddress", 2, 2, false);
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 2, false);*/

         sendWithProperty(0, "queues.testaddress", 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAll(10, 0);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingTimeout() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(new GroupingHandler()
         {
            public SimpleString getName()
            {
               return null;
            }

            public Response propose(Proposal proposal) throws Exception
            {
               return null;
            }

            public void proposed(Response response) throws Exception
            {

            }

            public void send(Response response, int distance) throws Exception
            {

            }

            public Response receive(Proposal proposal, int distance) throws Exception
            {
               return null;
            }

            public void onNotification(Notification notification)
            {
               
            }
         }, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2, 1);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);

         /*waitForBindings(0, "queues.testaddress", 2, 2, false);
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 2, false);*/

         try
         {
            sendWithProperty(1, "queues.testaddress", 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));
            fail("should timeout");
         }
         catch (Exception e)
         {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }


         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingSendTo2queues() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);

         /*waitForBindings(0, "queues.testaddress", 2, 2, false);
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 2, false);*/

         sendInRange(0, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 0);
         sendInRange(1, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(10, 20, 0);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingSendTo3queues() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 2, 2, false);
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 2, false);

         sendInRange(0, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 0);
         sendInRange(1, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(10, 20, 0);
         sendInRange(2, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(10, 20, 0);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingSendTo3queuesRemoteArbitrator() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(1, 1, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 0, true);

         waitForBindings(0, "queues.testaddress", 2, 1, false);
         waitForBindings(1, "queues.testaddress", 2, 0, false);
         waitForBindings(2, "queues.testaddress", 2, 1, false);

         sendInRange(1, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 1);
         sendInRange(2, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(10, 20, 1);
         sendInRange(0, "queues.testaddress", 20, 30, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(20, 30, 1);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingSendTo3queuesNoConsumerOnLocalQueue() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         //addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 0, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 2, 1, false);
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 1, false);

         sendInRange(1, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 0);
         sendInRange(2, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(10, 20, 0);
         sendInRange(0, "queues.testaddress", 20, 30, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(20, 30, 0);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }



   public void testGroupingRoundRobin() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 2, 2, false);
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 2, false);


         sendInRange(0, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));
         sendInRange(0, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id2"));
         sendInRange(0, "queues.testaddress", 20, 30, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id3"));
         verifyReceiveAllWithGroupIDRoundRobin(0, 10, 0, 1, 2);


         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }


   public void testGroupingSendTo3queuesQueueRemoved() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 2, 2, false);
         waitForBindings(1, "queues.testaddress", 2, 2, false);
         waitForBindings(2, "queues.testaddress", 2, 2, false);

         sendInRange(0, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 0);
         sendInRange(1, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(10, 20, 0);
         sendInRange(2, "queues.testaddress", 20, 30, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(20, 30, 0);
         removeConsumer(0);
         removeConsumer(1);
         removeConsumer(2);
         deleteQueue(0, "queue0");
         deleteQueue(1, "queue0");
         deleteQueue(2, "queue0");
         createQueue(0, "queues.testaddress", "queue1", null, false);
         addConsumer(3, 0, "queue1", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, false);
         waitForBindings(2, "queues.testaddress", 1, 1, false);

         sendInRange(0, "queues.testaddress", 30, 40, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         sendInRange(1, "queues.testaddress", 40, 50, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         sendInRange(2, "queues.testaddress", 50, 60, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));
         verifyReceiveAllInRange(30, 50, 3);
         System.out.println("*****************************************************************************");
      }
      finally
      {
         closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingSendTo3queuesPinnedNodeGoesDown() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, true);
         createQueue(1, "queues.testaddress", "queue0", null, true);
         createQueue(2, "queues.testaddress", "queue0", null, true);

         addConsumer(0, 1, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 0, true);

         waitForBindings(0, "queues.testaddress", 2, 1, false);
         waitForBindings(1, "queues.testaddress", 2, 0, false);
         waitForBindings(2, "queues.testaddress", 2, 1, false);

         sendInRange(1, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 0);

         stopServers(1);

         sendInRange(2, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         startServers(1);

         addConsumer(1, 1, "queue0", null);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         

         verifyReceiveAllInRange(10, 20, 1);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         //closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingSendTo3queuesPinnedNodeGoesDownSendBeforeStop() throws Exception
      {
         setupServer(0, isFileStorage(), isNetty());
         setupServer(1, isFileStorage(), isNetty());
         setupServer(2, isFileStorage(), isNetty());

         setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

         setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

         setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

         startServers(0, 1, 2);

         try
         {
            setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
            setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
            setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

            setupSessionFactory(0, isNetty());
            setupSessionFactory(1, isNetty());
            setupSessionFactory(2, isNetty());

            createQueue(0, "queues.testaddress", "queue0", null, true);
            createQueue(1, "queues.testaddress", "queue0", null, true);
            createQueue(2, "queues.testaddress", "queue0", null, true);

            addConsumer(0, 1, "queue0", null);

            waitForBindings(0, "queues.testaddress", 1, 0, true);
            waitForBindings(1, "queues.testaddress", 1, 1, true);
            waitForBindings(2, "queues.testaddress", 1, 0, true);

            waitForBindings(0, "queues.testaddress", 2, 1, false);
            waitForBindings(1, "queues.testaddress", 2, 0, false);
            waitForBindings(2, "queues.testaddress", 2, 1, false);

            sendInRange(1, "queues.testaddress", 0, 10, true, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

            verifyReceiveAllInRange(true, 0, 10, 0);

            closeAllConsumers();

            sendInRange(2, "queues.testaddress", 10, 20, true, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));


            sendInRange(0, "queues.testaddress", 20, 30, true, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

            stopServers(1);

            startServers(1);

            addConsumer(1, 1, "queue0", null);

            waitForBindings(1, "queues.testaddress", 1, 1, true);


            verifyReceiveAllInRange(10, 30, 1);


            System.out.println("*****************************************************************************");
         }
         finally
         {
            //closeAllConsumers();

            closeAllSessionFactories();

            stopServers(0, 1, 2);
         }
      }


   public void testGroupingSendTo3queuesPinnedNodeGoesDownSendAfterRestart() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, true);
         createQueue(1, "queues.testaddress", "queue0", null, true);
         createQueue(2, "queues.testaddress", "queue0", null, true);

         addConsumer(0, 1, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 0, true);

         waitForBindings(0, "queues.testaddress", 2, 1, false);
         waitForBindings(1, "queues.testaddress", 2, 0, false);
         waitForBindings(2, "queues.testaddress", 2, 1, false);

         sendInRange(1, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 0);

         stopServers(1);



         startServers(1);

         sendInRange(2, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));


         addConsumer(1, 1, "queue0", null);
         waitForBindings(1, "queues.testaddress", 1, 1, true);


         verifyReceiveAllInRange(10, 20, 1);


         sendInRange(0, "queues.testaddress", 20, 30, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));
         verifyReceiveAllInRange(20, 30, 1);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         //closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingSendTo3queuesSendingNodeGoesDown() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      try
      {
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
         setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, true);
         createQueue(1, "queues.testaddress", "queue0", null, true);
         createQueue(2, "queues.testaddress", "queue0", null, true);

         addConsumer(0, 1, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 0, true);

         waitForBindings(0, "queues.testaddress", 2, 1, false);
         waitForBindings(1, "queues.testaddress", 2, 0, false);
         waitForBindings(2, "queues.testaddress", 2, 1, false);

         sendInRange(1, "queues.testaddress", 0, 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(0, 10, 0);

         stopServers(0);

         sendInRange(2, "queues.testaddress", 10, 20, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));


         

         startServers(0);

         waitForBindings(0, "queues.testaddress", 1, 0, true);
         sendInRange(0, "queues.testaddress", 20, 30, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

         verifyReceiveAllInRange(10, 20, 0);
         verifyReceiveAllInRange(20, 30, 0);

         System.out.println("*****************************************************************************");
      }
      finally
      {
         //closeAllConsumers();

         closeAllSessionFactories();

         stopServers(0, 1, 2);
      }
   }

   public void testGroupingMultipleQueuesOnAddress() throws Exception
      {
         setupServer(0, isFileStorage(), isNetty());
         setupServer(1, isFileStorage(), isNetty());
         setupServer(2, isFileStorage(), isNetty());

         setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

         setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

         setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

         startServers(0, 1, 2);

         try
         {
            setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
            setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
            setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

            setupSessionFactory(0, isNetty());
            setupSessionFactory(1, isNetty());
            setupSessionFactory(2, isNetty());

            createQueue(0, "queues.testaddress", "queue0", null, false);
            createQueue(1, "queues.testaddress", "queue0", null, false);
            createQueue(2, "queues.testaddress", "queue0", null, false);

            createQueue(0, "queues.testaddress", "queue1", null, false);
            createQueue(1, "queues.testaddress", "queue1", null, false);
            createQueue(2, "queues.testaddress", "queue1", null, false);

            addConsumer(0, 0, "queue0", null);
            addConsumer(1, 1, "queue0", null);
            addConsumer(2, 2, "queue0", null);
            
            addConsumer(3, 0, "queue0", null);
            addConsumer(4, 1, "queue0", null);
            addConsumer(5, 2, "queue0", null);

            waitForBindings(0, "queues.testaddress", 2, 2, true);
            waitForBindings(1, "queues.testaddress", 2, 2, true);
            waitForBindings(2, "queues.testaddress", 2, 2, true);

            waitForBindings(0, "queues.testaddress", 4, 4, false);
            waitForBindings(1, "queues.testaddress", 4, 4, false);
            waitForBindings(2, "queues.testaddress", 4, 4, false);


            sendWithProperty(0, "queues.testaddress", 10, false, MessageImpl.HDR_GROUP_ID, new SimpleString("id1"));

            verifyReceiveAll(10, 0);

            System.out.println("*****************************************************************************");
         }
         finally
         {
            closeAllConsumers();

            closeAllSessionFactories();

            stopServers(0, 1, 2);
         }
      }

   public boolean isNetty()
   {
      return true;
   }

   public boolean isFileStorage()
   {
      return true;
   }
}

