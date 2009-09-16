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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A FailoverTest
 * 
 * Tests:
 * 
 * Failover via shared storage manager:
 * 
 * 
 * 5) Failover due to failure on create session
 * 
 * 6) Replicate above tests on JMS API
 * 
 * 7) Repeat above tests using replicated journal
 * 
 * 8) Test with different values of auto commit acks and autocomit sends
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FailoverTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(FailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private HornetQServer server0Service;

   private HornetQServer server1Service;

   private Map<String, Object> server1Params = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testNonTransacted() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertEquals("message" + i, message.getBody().readString());

            assertEquals(i, message.getProperty("counter"));

            message.acknowledge();
         }
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesSentSoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session.commit();

         fail("Should throw exception");
      }
      catch (HornetQException e)
      {
         assertEquals(HornetQException.TRANSACTION_ROLLED_BACK, e.getCode());
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receive(500);

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesNotSentSoNoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.commit();

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      // committing again should work since didn't send anything since last commit

      session.commit();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertEquals("message" + i, message.getBody().readString());

            assertEquals(i, message.getProperty("counter"));

            message.acknowledge();
         }
      }

      session.commit();

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesConsumedSoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));

         message.acknowledge();
      }

      RemotingConnection conn = ((ClientSessionInternal)session2).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session2.commit();

         fail("Should throw exception");
      }
      catch (HornetQException e)
      {
         assertEquals(HornetQException.TRANSACTION_ROLLED_BACK, e.getCode());
      }

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testTransactedMessagesNotConsumedSoNoRollback() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(true);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));

         message.acknowledge();
      }

      session2.commit();

      consumer.close();

      RemotingConnection conn = ((ClientSessionInternal)session2).getConnection();

      log.info("Failing connection**");

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      log.info("** creating the consumer");

      consumer = session2.createConsumer(ADDRESS);

      for (int i = numMessages / 2; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         log.info("got message " + message);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));

         message.acknowledge();
      }

      session2.commit();

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesSentSoRollbackOnEnd() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session.end(xid, XAResource.TMSUCCESS);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receive(500);

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesSentSoRollbackOnPrepare() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session.prepare(xid);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receive(500);

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // This might happen if 1PC optimisation kicks in
   public void testXAMessagesSentSoRollbackOnCommit() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session.commit(xid, true);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = consumer.receive(500);

      assertNull(message);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesNotSentSoNoRollbackOnCommit() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.commit(xid, false);

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      Xid xid2 = new XidImpl("tfytftyf".getBytes(), 54654, "iohiuohiuhgiu".getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertEquals("message" + i, message.getBody().readString());

            assertEquals(i, message.getProperty("counter"));

            message.acknowledge();
         }
      }

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesConsumedSoRollbackOnEnd() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(true, false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));

         message.acknowledge();
      }

      RemotingConnection conn = ((ClientSessionInternal)session2).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session2.end(xid, XAResource.TMSUCCESS);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testXAMessagesConsumedSoRollbackOnPrepare() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(true, false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));

         message.acknowledge();
      }

      session2.end(xid, XAResource.TMSUCCESS);

      RemotingConnection conn = ((ClientSessionInternal)session2).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session2.prepare(xid);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // 1PC optimisation
   public void testXAMessagesConsumedSoRollbackOnCommit() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session1 = sf.createSession(false, false);

      session1.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session1.addFailureListener(new MyListener());

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session1.commit();

      ClientSession session2 = sf.createSession(true, false, false);

      ClientConsumer consumer = session2.createConsumer(ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));

         message.acknowledge();
      }

      session2.end(xid, XAResource.TMSUCCESS);

      session2.prepare(xid);

      RemotingConnection conn = ((ClientSessionInternal)session2).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session2.commit(xid, true);

         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XA_RBOTHER, e.errorCode);
      }

      session1.close();

      session2.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testCreateNewFactoryAfterFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sendAndConsume(sf);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      conn.addFailureListener(new MyListener());

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      session.close();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   server1Params));

      session = sendAndConsume(sf);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailoverMultipleSessionsWithConsumers() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      final int numSessions = 10;

      final int numConsumersPerSession = 5;

      Map<ClientSession, List<ClientConsumer>> sessionConsumerMap = new HashMap<ClientSession, List<ClientConsumer>>();

      class MyListener implements FailureListener
      {
         CountDownLatch latch = new CountDownLatch(1);

         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      List<MyListener> listeners = new ArrayList<MyListener>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(true, true);

         List<ClientConsumer> consumers = new ArrayList<ClientConsumer>();

         for (int j = 0; j < numConsumersPerSession; j++)
         {
            SimpleString queueName = new SimpleString("queue" + i + "-" + j);

            session.createQueue(ADDRESS, queueName, null, true);

            ClientConsumer consumer = session.createConsumer(queueName);

            consumers.add(consumer);
         }

         sessionConsumerMap.put(session, consumers);
      }

      ClientSession sendSession = sf.createSession(true, true);

      ClientProducer producer = sendSession.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sendSession.createClientMessage(true);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      RemotingConnection conn = ((ClientSessionInternal)sendSession).getConnection();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      for (MyListener listener : listeners)
      {
         boolean ok = listener.latch.await(1000, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      for (ClientSession session : sessionConsumerMap.keySet())
      {
         session.start();
      }

      for (List<ClientConsumer> consumerList : sessionConsumerMap.values())
      {
         for (ClientConsumer consumer : consumerList)
         {
            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage message = consumer.receive(1000);

               assertNotNull(message);

               assertEquals("message" + i, message.getBody().readString());

               assertEquals(i, message.getProperty("counter"));

               message.acknowledge();
            }
         }
      }

      for (ClientSession session : sessionConsumerMap.keySet())
      {
         session.close();
      }

      sendSession.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailoverFailMultipleUnderlyingConnections() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      class MyListener implements FailureListener
      {
         CountDownLatch latch = new CountDownLatch(1);

         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      ClientSession session1 = sf.createSession(true, true);
      ClientSession session2 = sf.createSession(true, true);
      ClientSession session3 = sf.createSession(true, true);

      SimpleString queueName1 = new SimpleString("queue1");
      session1.createQueue(ADDRESS, queueName1, null, true);
      MyListener listener1 = new MyListener();
      session1.addFailureListener(listener1);

      SimpleString queueName2 = new SimpleString("queue2");
      session2.createQueue(ADDRESS, queueName2, null, true);
      MyListener listener2 = new MyListener();
      session2.addFailureListener(listener2);

      SimpleString queueName3 = new SimpleString("queue3");
      session3.createQueue(ADDRESS, queueName3, null, true);
      MyListener listener3 = new MyListener();
      session3.addFailureListener(listener3);

      ClientConsumer consumer1 = session1.createConsumer(queueName1);
      ClientConsumer consumer2 = session1.createConsumer(queueName2);
      ClientConsumer consumer3 = session1.createConsumer(queueName3);

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(true);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      // Fail all the connections

      RemotingConnection conn1 = ((ClientSessionInternal)session1).getConnection();
      RemotingConnection conn2 = ((ClientSessionInternal)session2).getConnection();
      RemotingConnection conn3 = ((ClientSessionInternal)session3).getConnection();

      assertTrue(conn1 != conn2);
      assertTrue(conn2 != conn3);
      assertTrue(conn1 != conn3);

      conn2.fail(new HornetQException(HornetQException.NOT_CONNECTED));
      conn3.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = listener1.latch.await(1000, TimeUnit.MILLISECONDS);
      assertTrue(ok);
      ok = listener2.latch.await(1000, TimeUnit.MILLISECONDS);
      assertTrue(ok);
      ok = listener3.latch.await(1000, TimeUnit.MILLISECONDS);
      assertTrue(ok);

      session1.start();
      session2.start();
      session3.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);
         assertNotNull(message);
         assertEquals("message" + i, message.getBody().readString());
         assertEquals(i, message.getProperty("counter"));
         message.acknowledge();

         message = consumer2.receive(1000);
         assertNotNull(message);
         assertEquals("message" + i, message.getBody().readString());
         assertEquals(i, message.getProperty("counter"));
         message.acknowledge();

         message = consumer3.receive(1000);
         assertNotNull(message);
         assertEquals("message" + i, message.getBody().readString());
         assertEquals(i, message.getProperty("counter"));
         message.acknowledge();
      }

      session1.close();
      session2.close();
      session3.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      Configuration config1 = super.createDefaultConfig();
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations()
             .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", server1Params));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      server1Service = super.createServer(true, config1);

      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations()
             .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      server0Service = super.createServer(true, config0);

      server1Service.start();
      server0Service.start();
   }

   protected void tearDown() throws Exception
   {
      server1Service.stop();

      server0Service.stop();

      assertEquals(0, InVMRegistry.instance.size());

      server1Service = null;

      server0Service = null;

      server1Params = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private ClientSession sendAndConsume(final ClientSessionFactory sf) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      assertNull(message3);

      return session;
   }

   /*
    * Browser will get reset to beginning after failover
    */
   public void testFailWithBrowser() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS, true);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      log.info("after failover");

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            log.info("got message " + i);

            assertNotNull(message);

            assertEquals("message" + i, message.getBody().readString());

            assertEquals(i, message.getProperty("counter"));

            message.acknowledge();
         }
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailThenReceiveMoreMessagesAfterFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      // Should get the same ones after failover since we didn't ack

      for (int i = 0; i < numMessages; i++)
      {
         // Only the persistent messages will survive

         if (i % 2 == 0)
         {
            ClientMessage message = consumer.receive(1000);

            assertNotNull(message);

            assertEquals("message" + i, message.getBody().readString());

            assertEquals(i, message.getProperty("counter"));

            message.acknowledge();
         }
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailThenReceiveMoreMessagesAfterFailover2() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      sf.setBlockOnAcknowledge(true);

      ClientSession session = sf.createSession(true, true, 0);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));
        
         message.acknowledge();
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      // Send some more

      for (int i = numMessages; i < numMessages * 2; i++)
      {
         ClientMessage message = session.createClientMessage(i % 2 == 0);

         message.getBody().writeString("message" + i);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      // Should get the same ones after failover since we didn't ack

      for (int i = numMessages; i < numMessages * 2; i++)
      {
         ClientMessage message = consumer.receive(1000);

         assertNotNull(message);

         assertEquals("message" + i, message.getBody().readString());

         assertEquals(i, message.getProperty("counter"));

         message.acknowledge();
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }
        
   public void testForceBlockingReturn() throws Exception
   {      
      server0Service.stop();
      
      //Add an interceptor to delay the send method so we can get time to cause failover before it returns
      
      server0Service.getConfiguration().getInterceptorClassNames().add(DelayInterceptor.class.getCanonicalName());
      
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                server1Params));
      
      server0Service.start();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);
      sf.setBlockOnAcknowledge(true);

      final ClientSession session = sf.createSession(true, true, 0);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {
            latch.countDown();
         }
      }

      session.addFailureListener(new MyListener());

      final ClientProducer producer = session.createProducer(ADDRESS);
      
      class Sender extends Thread
      {
         public void run()
         {
            ClientMessage message = session.createClientMessage(true);

            message.getBody().writeString("message");

            try
            {
               producer.send(message);
            }
            catch (HornetQException e)
            {
               this.e = e;
            }
         }
         
         volatile HornetQException e;
      }
      
      Sender sender = new Sender();
      
      sender.start();
      
      Thread.sleep(500);
      
      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);
      
      sender.join();
      
      assertNotNull(sender.e);
      
      assertEquals(sender.e.getCode(), HornetQException.UNBLOCKED);
                  
      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
      
      
   }
   
   
   // Inner classes -------------------------------------------------
}
