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
package org.hornetq.tests.integration.client;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ReceiveImmediateTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ReceiveImmediateTest.class);

   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ReceiveImmediateTest.queue");

   private final SimpleString ADDRESS = new SimpleString("ReceiveImmediateTest.address");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig(false);
      server = HornetQServers.newHornetQServer(config, false);

      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   private ClientSessionFactory sf;

   public void testConsumerReceiveImmediateWithNoMessages() throws Exception
   {
      doConsumerReceiveImmediateWithNoMessages(false);
   }
   
   public void testConsumerReceiveImmediate() throws Exception
   {
      doConsumerReceiveImmediate(false);
   }

   public void testBrowserReceiveImmediateWithNoMessages() throws Exception
   {
      doConsumerReceiveImmediateWithNoMessages(true);
   }

   public void testBrowserReceiveImmediate() throws Exception
   {
      doConsumerReceiveImmediate(true);
   }

   public void testConsumerReceiveImmediateWithSessionStop() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      sf = locator.createSessionFactory();
      sf.getServerLocator().setBlockOnNonDurableSend(true);
      sf.getServerLocator().setBlockOnAcknowledge(true);
      sf.getServerLocator().setAckBatchSize(0);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false);
      session.start();

      session.stop();
      Assert.assertNull(consumer.receiveImmediate());

      session.start();
      long start = System.currentTimeMillis();
      ClientMessage msg = consumer.receive(2000);
      long end = System.currentTimeMillis();
      Assert.assertNull(msg);
      // we waited for at least 2000ms
      Assert.assertTrue("waited only " + (end - start), end - start >= 2000);

      consumer.close();

      session.close();

      sf.close();

   }

   private void doConsumerReceiveImmediateWithNoMessages(final boolean browser) throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      sf = locator.createSessionFactory();
      sf.getServerLocator().setBlockOnNonDurableSend(true);
      sf.getServerLocator().setBlockOnAcknowledge(true);
      sf.getServerLocator().setAckBatchSize(0);

      ClientSession session = sf.createSession(false, true, false);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, browser);
      session.start();

      ClientMessage message = consumer.receiveImmediate();
      Assert.assertNull(message);

      session.close();

      sf.close();
   }

   private void doConsumerReceiveImmediate(final boolean browser) throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      sf = locator.createSessionFactory();
      sf.getServerLocator().setBlockOnNonDurableSend(true);
      sf.getServerLocator().setBlockOnAcknowledge(true);
      sf.getServerLocator().setAckBatchSize(0);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, browser);
      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receiveImmediate();
         Assert.assertNotNull("did not receive message " + i, message2);
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (!browser)
         {
            message2.acknowledge();
         }
      }

      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());

      Assert.assertNull(consumer.receiveImmediate());

      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      int messagesOnServer = browser ? numMessages : 0;
      Assert.assertEquals(messagesOnServer,
                          ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      consumer.close();

      session.close();

      sf.close();

   }

}
