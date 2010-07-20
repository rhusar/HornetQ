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

package org.hornetq.tests.integration.paging;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A SendTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PagingSendTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   public static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected boolean isNetty()
   {
      return false;
   }

   private HornetQServer newHornetQServer()
   {
      HornetQServer server = super.createServer(true, isNetty());

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(100 * 1024);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   public void testSameMessageOverAndOverBlocking() throws Exception
   {
      dotestSameMessageOverAndOver(true);
   }

   public void testSameMessageOverAndOverNonBlocking() throws Exception
   {
      dotestSameMessageOverAndOver(false);
   }

   public void dotestSameMessageOverAndOver(final boolean blocking) throws Exception
   {
      HornetQServer server = newHornetQServer();

      server.start();

      try
      {
         ServerLocator locator = createFactory(isNetty());
         ClientSessionFactory sf;



         // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
         // page mode
         // and we could only guarantee that by setting it to synchronous
         locator.setBlockOnNonDurableSend(blocking);
         locator.setBlockOnDurableSend(blocking);
         locator.setBlockOnAcknowledge(blocking);
         sf = locator.createSessionFactory() ;
         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingSendTest.ADDRESS, PagingSendTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingSendTest.ADDRESS);

         ClientMessage message = null;

         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(new byte[1024]);

         for (int i = 0; i < 200; i++)
         {
            producer.send(message);
         }

         session.close();

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(PagingSendTest.ADDRESS);

         session.start();

         for (int i = 0; i < 200; i++)
         {
            ClientMessage message2 = consumer.receive(10000);

            Assert.assertNotNull(message2);

            if (i == 100)
            {
               session.commit();
            }

            message2.acknowledge();
         }

         consumer.close();

         session.close();

         locator.close();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}