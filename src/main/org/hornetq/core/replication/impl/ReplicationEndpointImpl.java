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

package org.hornetq.core.replication.impl;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.NullResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.server.HornetQServer;

/**
 * A ReplicationPacketHandler
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationEndpointImpl implements ReplicationEndpoint
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationEndpointImpl.class);

   // Attributes ----------------------------------------------------

   private final HornetQServer server;

   private Channel channel;

   private Journal bindingsJournal;

   private Journal messagingJournal;
   
   private JournalStorageManager storage;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public ReplicationEndpointImpl(HornetQServer server)
   {
      this.server = server;
   }

   // Public --------------------------------------------------------
   /* 
    * (non-Javadoc)
    * @see org.hornetq.core.remoting.ChannelHandler#handlePacket(org.hornetq.core.remoting.Packet)
    */
   public void handlePacket(Packet packet)
   {
      try
      {
         if (packet.getType() == PacketImpl.REPLICATION_APPEND)
         {
            System.out.println("Replicated");
            handleAppendAddRecord(packet);
         }
      }
      catch (Exception e)
      {
         // TODO: what to do when the IO fails on the backup side? should we shutdown the backup?
         log.warn(e.getMessage(), e);
      }
      channel.send(new ReplicationResponseMessage());
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      Configuration config = server.getConfiguration();
      
      // TODO: this needs an executor
      JournalStorageManager storage = new JournalStorageManager(config, null);
      storage.start();
      
      this.bindingsJournal = storage.getBindingsJournal();
      this.messagingJournal = storage.getBindingsJournal();
      
      // We only need to load internal structures on the backup...
      storage.loadInternalOnly();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      storage.stop();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationEndpoint#getChannel()
    */
   public Channel getChannel()
   {
      return channel;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationEndpoint#setChannel(org.hornetq.core.remoting.Channel)
    */
   public void setChannel(Channel channel)
   {
      this.channel = channel;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * @param packet
    * @throws Exception
    */
   private void handleAppendAddRecord(Packet packet) throws Exception
   {
      ReplicationAddMessage addMessage = (ReplicationAddMessage)packet;
      Journal journalToUse;

      if (addMessage.getJournalID() == (byte)0)
      {
         journalToUse = bindingsJournal;
      }
      else
      {
         journalToUse = messagingJournal;
      }

      journalToUse.appendAddRecord(addMessage.getId(), addMessage.getRecordType(), addMessage.getRecordData(), false);
   }

   // Inner classes -------------------------------------------------

}
