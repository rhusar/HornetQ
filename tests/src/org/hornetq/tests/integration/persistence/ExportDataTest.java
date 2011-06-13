/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.persistence.tools.ManageDataTool;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * UnitTest for importing/exporting journal messages.
 * <p/>
 * Scenario:
 *
 * @author Torben Jaeger</a>
 * @author Clebert Suconic</a>
 */
public class ExportDataTest extends ServiceTestBase {

   private static final int MSG_SIZE = 1024;

   public void testExportImport() throws Exception {

      HornetQServer server = createServer(true);
      
      
      server.start();
      
      ServerLocator locator = createNonHALocator(false);
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = null;
      
      try
      {
         session = sf.createSession();
         
         session.createQueue("ADDRESS", "Q1");
         session.createQueue("ADDRESS", "Q2");
         
         ClientProducer prod = session.createProducer("ADDRESS");
         
         for (int i = 0 ; i < 100; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.putStringProperty("prop", "TST1");
            msg.putIntProperty("count", i);
            for (int b = 0; b < MSG_SIZE; b++)
            {
               msg.getBodyBuffer().writeByte(getSamplebyte(b));
            }
            prod.send(msg);
         }
         
         session.start();
         ClientConsumer cons = session.createConsumer("Q1");
         
         for (int i = 0; i < 50; i++)
         {
            ClientMessage msg = cons.receive(1000);
            assertNotNull(msg);
            msg.acknowledge();
            assertEquals(i, msg.getIntProperty("count").intValue());
         }
         
         session.commit();
         
         session.close();
         sf.close();
         locator.close();
         
         server.stop();
         
         ByteArrayOutputStream bout = new ByteArrayOutputStream();
         
         ManageDataTool.exportMessages(getBindingsDir(), getJournalDir(), bout);
         
         InputStream is = new ByteArrayInputStream(bout.toByteArray());
         
         clearData();
         
         server = createServer(true);
         
         server.start();
         
         locator = createInVMNonHALocator();
         
         ManageDataTool.importMessages(is, locator, "a", "b");
         
         ClientSessionFactory csf = locator.createSessionFactory();
         
         session = csf.createSession();
         session.start();
         cons = session.createConsumer("Q1"); 

         for (int i = 50; i < 100; i++)
         {
            ClientMessage msg = cons.receive(1000);
            assertNotNull(msg);
            assertEquals(MSG_SIZE, msg.getBodyBuffer().readableBytes());
            for (int b = 0; b < msg.getBodyBuffer().readableBytes(); b++) {
               assertEquals(getSamplebyte(b), msg.getBodyBuffer().readByte());
            }
            msg.acknowledge();
            assertEquals(i, msg.getIntProperty("count").intValue());
         }
         
         server.stop();
         
      }
      finally
      {
         sf.close();
         locator.close();
      }

   }

}
