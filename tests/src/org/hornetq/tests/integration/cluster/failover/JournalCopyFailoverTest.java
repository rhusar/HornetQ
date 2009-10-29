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

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.utils.SimpleString;

/**
 * A JournalCopyFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCopyFailoverTest extends FailoverTestBase
{

   // Constants -----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSynchronousCopy() throws Exception
   {
      ClientSessionFactoryInternal sf = getSessionFactory();

      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = session.createClientMessage(true);
         msg.setBody(ChannelBuffers.wrappedBuffer(new byte[512]));
         prod.send(msg);
      }

      server0Service.configureBackup("org.hornetq.integration.transports.netty.NettyConnectorFactory",
                                     "hornetq.remoting.netty.port=" + (org.hornetq.integration.transports.netty.TransportConstants.DEFAULT_PORT + 1),
                                     true);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   protected void createReplicatedConfigs() throws Exception
   {
      Configuration config1 = super.createDefaultConfig();
      config1.setBindingsDirectory(config1.getBindingsDirectory() + "_backup");
      config1.setJournalDirectory(config1.getJournalDirectory() + "_backup");
      config1.setPagingDirectory(config1.getPagingDirectory() + "_backup");
      config1.setLargeMessagesDirectory(config1.getLargeMessagesDirectory() + "_backup");
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(false);
      config1.setBackup(true);
      server1Service = super.createServer(true, config1);

      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      getConnectorTransportConfiguration(false);
      // config0.getConnectorConfigurations().put("toBackup", getConnectorTransportConfiguration(false));
      // config0.setBackupConnectorName("toBackup");
      // config0.setSecurityEnabled(false);
      // config0.setSharedStore(false);
      server0Service = super.createServer(true, config0);

      server1Service.start();
      server0Service.start();
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      return getNettyAcceptorTransportConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return getNettyConnectorTransportConfiguration(live);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
