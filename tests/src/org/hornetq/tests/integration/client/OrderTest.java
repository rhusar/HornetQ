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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A OrderTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class OrderTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   protected void setUp() throws Exception
   {
      super.setUp();
      server = createServer(true, true);
      server.getConfiguration().setJournalFileSize(10 * 1024 * 1024);
      server.start();
   }

   protected void tearDown() throws Exception
   {
      server.stop();
      super.tearDown();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSimpleOrder() throws Exception
   {
      ClientSessionFactory sf = createNettyFactory();

      sf.setBlockOnNonPersistentSend(false);
      sf.setBlockOnPersistentSend(false);
      sf.setBlockOnAcknowledge(true);

      ClientSession session = sf.createSession(true, true, 0);

      try
      {
         session.createQueue("queue", "queue", true);

         ClientProducer prod = session.createProducer("queue");

         for (int i = 0; i < 100; i++)
         {
            ClientMessage msg = session.createClientMessage(i % 2 == 0);
            msg.setBody(session.createBuffer(new byte[1024]));
            msg.putIntProperty("id", i);
            prod.send(msg);
         }

         session.close();

         boolean started = false;

         for (int start = 0; start < 3; start++)
         {

            if (start == 2)
            {
               started = true;
               server.stop();
               server.start();
            }

            session = sf.createSession(true, true);

            session.start();

            ClientConsumer cons = session.createConsumer("queue");

            for (int i = 0; i < 100; i++)
            {
               if (!started || started && i % 2 == 0)
               {
                  ClientMessage msg = cons.receive(10000);
                  assertEquals(i, msg.getIntProperty("id").intValue());
               }
            }

            cons.close();

            cons = session.createConsumer("queue");

            for (int i = 0; i < 100; i++)
            {
               if (!started || started && i % 2 == 0)
               {
                  ClientMessage msg = cons.receive(10000);
                  assertEquals(i, msg.getIntProperty("id").intValue());
               }
            }

            session.close();
         }

      }
      finally
      {
         sf.close();
         session.close();
      }

   }

   private void fail(ClientSession session) throws InterruptedException
   {

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements SessionFailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }

         public void beforeReconnect(HornetQException exception)
         {
         }
      }

      MyListener listener = new MyListener();
      session.addFailureListener(listener);

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      session.removeFailureListener(listener);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
