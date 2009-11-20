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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * The basic test possible. Useful to validate basic stuff.
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * TODO: Don't commit this test on the main branch
 */
public class SimpleClientTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testSimpleTestNoTransacted() throws Exception
   {
      HornetQServer server = createServer(true);
      
      server.start();

      ClientSessionFactory factory = createInVMFactory();
      
      ClientSession session = null;
      
      try
      {
         session = factory.createSession(false, true, true);
         
         session.createQueue("A", "A", true);
         ClientProducer producer = session.createProducer("A");
         producer.send(session.createClientMessage(true));
         
         session.start();
         
         ClientConsumer cons = session.createConsumer("A");
         
         ClientMessage msg = cons.receive(10000);
         assertNotNull(msg);
         msg.acknowledge();
         
      }
      finally
      {
         session.close();
         factory.close();
         server.stop();
      }
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
