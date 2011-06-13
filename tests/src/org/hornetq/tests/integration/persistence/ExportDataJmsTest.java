package org.hornetq.tests.integration.persistence;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.persistence.tools.ManageDataTool;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.JMSTestBase;

import javax.jms.*;
import javax.management.MBeanServerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: torben
 * Date: 13.06.11
 * Time: 12:57
 * To change this template use File | Settings | File Templates.
 */
public class ExportDataJmsTest extends JMSTestBase {

   public static final String QUEUE = "Q1";

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      jmsServer.stop();

      server = createServer(true, false,true);

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMContext();
      jmsServer.setContext(context);
      jmsServer.start();

      jmsServer.createQueue(false, QUEUE, null, true, QUEUE);
      cf = (ConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
   }

   @Override
   protected void tearDown() throws Exception
   {
      cf = null;

      super.tearDown();
   }

   public void testExportImportJmsMessages() throws Exception {
      Connection connection = cf.createConnection("a", "b");
      connection.start();

      Session jmsSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      Destination q = HornetQJMSClient.createQueue(QUEUE);

      MessageProducer producer = jmsSession.createProducer(q);

      TextMessage message = jmsSession.createTextMessage("foobar");
      producer.send(message);

      jmsSession.commit();
      producer.close();
      jmsSession.close();
      connection.close();

      jmsServer.stop();

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ManageDataTool.exportMessages(getBindingsDir(), getJournalDir(), bout);

      InputStream is = new ByteArrayInputStream(bout.toByteArray());

      clearData();

      server = createServer(true, false, true);
      server.start();

      ServerLocator locator = createNonHALocator(false);
      ManageDataTool.importMessages(is, locator, "a", "b");

      jmsServer = new JMSServerManagerImpl(server);
      jmsServer.setContext(context);
      jmsServer.start();

      Connection newConnection = cf.createConnection("a", "b");
      newConnection.start();

      Session newJmsSession = newConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      MessageConsumer consumer = newJmsSession.createConsumer(q);
      Message received = consumer.receive(1000);
      assertNotNull(received);
      assertNotNull(received.getJMSMessageID());
      assertEquals("foobar", ((TextMessage) received).getText());
      received.acknowledge();

      consumer.close();
      newJmsSession.close();
      newConnection.close();

   }
}
