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

package org.hornetq.tests.integration.client;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.JMSTestBase;

public class JMSPagingFileDeleteTest extends JMSTestBase
{
   static Logger log = Logger.getLogger(JMSPagingFileDeleteTest.class);

   Topic topic1;

   private static final int MESSAGE_SIZE = 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   private static final int PAGE_MAX = 20 * 1024;

   private static final int RECEIVE_TIMEOUT = 500;

   private static final int MESSAGE_NUM = 50;

   @Override
   protected boolean usePersistence()
   {
      return true;
   }

   @Override
   protected void setUp() throws Exception
   {
      clearData();
      super.setUp();

      topic1 = createTopic("topic1");

      // Paging Setting
      AddressSettings setting = new AddressSettings();
      setting.setPageSizeBytes(JMSPagingFileDeleteTest.PAGE_SIZE);
      setting.setMaxSizeBytes(JMSPagingFileDeleteTest.PAGE_MAX);
      server.getAddressSettingsRepository().addMatch("#", setting);
   }

   @Override
   protected void tearDown() throws Exception
   {
      topic1 = null;
      super.tearDown();
   }

   public void testTopics() throws Exception
   {
      Connection connection = null;

      try
      {
         connection = cf.createConnection();
         connection.setClientID("cid");

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(topic1);
         MessageConsumer subscriber1 = session.createDurableSubscriber(topic1, "subscriber-1");
         MessageConsumer subscriber2 = session.createDurableSubscriber(topic1, "subscriber-2");

         int numMessages = sendMessages(createMessage(session), producer);

         printPageStoreInfo();

         Assert.assertTrue(getPagingStore().isPaging());

         connection.start();

         // -----------------(Step2) Restart the server. --------------------------------------
         // If try this test without restarting server, please comment out this section;
         close(connection);
         stopAndStartServer();

         connection = cf.createConnection();
         connection.setClientID("cid");
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         subscriber1 = session.createDurableSubscriber(topic1, "subscriber-1");
         subscriber2 = session.createDurableSubscriber(topic1, "subscriber-2");
         connection.start();

         // -----------------(Step3) Subscribe to all the messages from the topic.--------------
         System.out.println("---------- Receive all messages. ----------");
         for (int i = 0; i < numMessages; i++)
         {
            Assert.assertNotNull(subscriber1.receive(JMSPagingFileDeleteTest.RECEIVE_TIMEOUT));
            Assert.assertNotNull(subscriber2.receive(JMSPagingFileDeleteTest.RECEIVE_TIMEOUT));
         }

         waitUntilPagingStops(5000);

         printPageStoreInfo();

         Assert.assertEquals(0, getPagingStore().getAddressSize());
         // assertEquals(1, pagingStore.getNumberOfPages()); //I expected number of the page is 1, but It was not.
         Assert.assertFalse(getPagingStore().isPaging()); // I expected IsPaging is false, but It was true.
         // If the server is not restart, this test pass.

         // -----------------(Step4) Publish a message. the message is stored in the paging file.
         producer = session.createProducer(topic1);
         sendMessage(createMessage(session), producer);

         printPageStoreInfo();

         Assert.assertEquals(1, getPagingStore().getNumberOfPages()); // I expected number of the page is 1, but It was
                                                                      // not.
      }
      finally
      {
         close(connection);
      }
   }

   public void testTopics_nonDurable() throws Exception
   {
      Connection connection = null;

      try
      {
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(topic1);

         printPageStoreInfo();

         connection.start();

         MessageConsumer subscriber = session.createConsumer(topic1);
         final Message message = createMessage(session);
         int numMessages = sendJustEnoughMessagesForPaging(message, producer);

         // ###Works if uncomment this to send one extra message or if use sendMessages instead above
         // printPageStoreInfo();
         sendMessage(message, producer);
         // numMessages++;

         printPageStoreInfo();

         for (int i = 0; i < numMessages; i++)
         {
            Assert.assertNotNull(subscriber.receive(JMSPagingFileDeleteTest.RECEIVE_TIMEOUT));
            System.out.println("Messages recd:" + (i + 1));
         }
         
         assertNull(subscriber.receive(1000));

         waitUntilPagingStops(5000);

         printPageStoreInfo();
      }
      finally
      {
         close(connection);
      }
   }

   private void close(final Connection connection)
   {
      try
      {
         if (connection != null)
         {
            connection.close();
         }
      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
   }

   private int sendMessages(final Message message, final MessageProducer producer) throws JMSException
   {
      System.out.println("---------- Send messages. ----------");
      for (int i = 0; i < JMSPagingFileDeleteTest.MESSAGE_NUM; i++)
      {
         sendMessage(message, producer);
      }
      System.out.println("Sent " + JMSPagingFileDeleteTest.MESSAGE_NUM + " messages.");

      return JMSPagingFileDeleteTest.MESSAGE_NUM;
   }

   private int sendJustEnoughMessagesForPaging(final Message message, final MessageProducer producer) throws Exception
   {
      int messagesSendCount = 0;
      while (!getPagingStore().isPaging())
      {
         sendMessage(message, producer);
         messagesSendCount++;
      }

      System.out.println(messagesSendCount + " messages sent before paging started");

      return messagesSendCount;
   }

   private void sendMessage(final Message message, final MessageProducer producer) throws JMSException
   {
      producer.send(message);
   }

   private Message createMessage(final Session session) throws JMSException
   {
      BytesMessage bytesMessage = session.createBytesMessage();
      bytesMessage.writeBytes(new byte[JMSPagingFileDeleteTest.MESSAGE_SIZE]);
      return bytesMessage;
   }

   private void waitUntilPagingStops(final int timeoutMillis) throws Exception, InterruptedException
   {
      long timeout = System.currentTimeMillis() + timeoutMillis;
      while (timeout > System.currentTimeMillis() && getPagingStore().isPaging())
      {
         Thread.sleep(100);
      }
      
      if (!getPagingStore().isPaging())
      {
         System.exit(-1);
      }
      Assert.assertFalse("Paging should have stopped", getPagingStore().isPaging());
   }

   private PagingStore getPagingStore() throws Exception
   {
      return server.getPagingManager().getPageStore(new SimpleString("jms.topic.topic1"));
   }

   private void printPageStoreInfo() throws Exception
   {
      PagingStore pagingStore = getPagingStore();
      System.out.println("---------- Paging Store Info ----------");
      System.out.println(" CurrentPage = " + pagingStore.getCurrentPage());
      System.out.println(" FirstPage = " + pagingStore.getFirstPage());
      System.out.println(" Number of Pages = " + pagingStore.getNumberOfPages());
      System.out.println(" Address Size = " + pagingStore.getAddressSize());
      System.out.println(" Is Paging = " + pagingStore.isPaging());
   }

   private void stopAndStartServer() throws Exception
   {
      System.out.println("---------- Restart server. ----------");

      jmsServer.stop();

      jmsServer.start();
      jmsServer.activated();
      registerConnectionFactory();

      printPageStoreInfo();
   }
}