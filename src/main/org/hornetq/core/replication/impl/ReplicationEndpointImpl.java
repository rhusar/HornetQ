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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.SimpleString;

/**
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

   private static final boolean trace = false;

   private final HornetQServer server;

   private Channel channel;

   private Journal bindingsJournal;

   private Journal messagingJournal;

   private JournalStorageManager storage;

   private PagingManager pageManager;

   private final ConcurrentMap<SimpleString, ConcurrentMap<Integer, Page>> pageIndex = new ConcurrentHashMap<SimpleString, ConcurrentMap<Integer, Page>>();

   // Constructors --------------------------------------------------
   public ReplicationEndpointImpl(final HornetQServer server)
   {
      this.server = server;
   }

   // Public --------------------------------------------------------
   /* 
    * (non-Javadoc)
    * @see org.hornetq.core.remoting.ChannelHandler#handlePacket(org.hornetq.core.remoting.Packet)
    */
   public void handlePacket(final Packet packet)
   {
      try
      {
         if (packet.getType() == PacketImpl.REPLICATION_APPEND)
         {
            handleAppendAddRecord((ReplicationAddMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_APPEND_TX)
         {
            handleAppendAddTXRecord((ReplicationAddTXMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_DELETE)
         {
            handleAppendDelete((ReplicationDeleteMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_DELETE_TX)
         {
            handleAppendDeleteTX((ReplicationDeleteTXMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_PREPARE)
         {
            handlePrepare((ReplicationPrepareMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_COMMIT_ROLLBACK)
         {
            handleCommitRollback((ReplicationCommitMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_PAGE_WRITE)
         {
            handlePageWrite((ReplicationPageWriteMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_PAGE_EVENT)
         {
            handlePageEvent((ReplicationPageEventMessage)packet);
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

      storage = new JournalStorageManager(config, server.getExecutorFactory().getExecutor());
      storage.start();

      bindingsJournal = storage.getBindingsJournal();
      messagingJournal = storage.getMessageJournal();

      // We only need to load internal structures on the backup...
      storage.loadInternalOnly();

      pageManager = new PagingManagerImpl(new PagingStoreFactoryNIO(config.getPagingDirectory(),
                                                                    server.getExecutorFactory()),
                                          storage,
                                          server.getAddressSettingsRepository(),
                                          false);

      pageManager.start();

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      channel.close();
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
   public void setChannel(final Channel channel)
   {
      this.channel = channel;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * @param packet
    */
   private void handleCommitRollback(final ReplicationCommitMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.isRollback())
      {
         journalToUse.appendRollbackRecord(packet.getTxId(), false);
      }
      else
      {
         journalToUse.appendCommitRecord(packet.getTxId(), false);
      }
   }

   /**
    * @param packet
    */
   private void handlePrepare(final ReplicationPrepareMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      journalToUse.appendPrepareRecord(packet.getTxId(), packet.getRecordData(), false);
   }

   /**
    * @param packet
    */
   private void handleAppendDeleteTX(final ReplicationDeleteTXMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      journalToUse.appendDeleteRecordTransactional(packet.getTxId(), packet.getId(), packet.getRecordData());
   }

   /**
    * @param packet
    */
   private void handleAppendDelete(final ReplicationDeleteMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      journalToUse.appendDeleteRecord(packet.getId(), false);
   }

   /**
    * @param packet
    */
   private void handleAppendAddTXRecord(final ReplicationAddTXMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.isUpdate())
      {
         journalToUse.appendUpdateRecordTransactional(packet.getTxId(),
                                                      packet.getId(),
                                                      packet.getRecordType(),
                                                      packet.getRecordData());
      }
      else
      {
         journalToUse.appendAddRecordTransactional(packet.getTxId(),
                                                   packet.getId(),
                                                   packet.getRecordType(),
                                                   packet.getRecordData());
      }
   }

   /**
    * @param packet
    * @throws Exception
    */
   private void handleAppendAddRecord(final ReplicationAddMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.isUpdate())
      {
         if (trace)
         {
            System.out.println("Endpoint appendUpdate id = " + packet.getId());
         }
         journalToUse.appendUpdateRecord(packet.getId(), packet.getRecordType(), packet.getRecordData(), false);
      }
      else
      {
         if (trace)
         {
            System.out.println("Endpoint append id = " + packet.getId());
         }
         journalToUse.appendAddRecord(packet.getId(), packet.getRecordType(), packet.getRecordData(), false);
      }
   }

   /**
    * @param packet
    */
   private void handlePageEvent(final ReplicationPageEventMessage packet) throws Exception
   {
      ConcurrentMap<Integer, Page> pages = getPageMap(packet.getStoreName());

      Page page = pages.remove(packet.getPageNumber());
      
      if (page == null)
      {
         page = getPage(packet.getStoreName(), packet.getPageNumber());
      }
      

      if (page != null)
      {
         if (packet.isDelete())
         {
            page.delete();
         }
         else
         {
            page.close();
         }
      }

   }

   /**
    * @param packet
    */
   private void handlePageWrite(final ReplicationPageWriteMessage packet) throws Exception
   {
      PagedMessage pgdMessage = packet.getPagedMessage();
      ServerMessage msg = pgdMessage.getMessage(storage);
      Page page = getPage(msg.getDestination(), packet.getPageNumber());
      page.write(pgdMessage);
   }

   private ConcurrentMap<Integer, Page> getPageMap(final SimpleString storeName)
   {
      ConcurrentMap<Integer, Page> resultIndex = pageIndex.get(storeName);

      if (resultIndex == null)
      {
         resultIndex = new ConcurrentHashMap<Integer, Page>();
         ConcurrentMap<Integer, Page> mapResult = pageIndex.putIfAbsent(storeName, resultIndex);
         if (mapResult != null)
         {
            resultIndex = mapResult;
         }
      }

      return resultIndex;
   }

   private Page getPage(final SimpleString storeName, final int pageId) throws Exception
   {
      ConcurrentMap<Integer, Page> map = getPageMap(storeName);

      Page page = map.get(pageId);

      if (page == null)
      {
         page = newPage(pageId, storeName, map);
      }

      return page;
   }

   /**
    * @param pageId
    * @param map
    * @return
    */
   private synchronized Page newPage(final int pageId,
                                     final SimpleString storeName,
                                     final ConcurrentMap<Integer, Page> map) throws Exception
   {
      Page page = map.get(pageId);

      if (page == null)
      {
         page = pageManager.getPageStore(storeName).createPage(pageId);
         page.open();
         map.put(pageId, page);
      }

      return page;
   }

   /**
    * @param journalID
    * @return
    */
   private Journal getJournal(final byte journalID)
   {
      Journal journalToUse;
      if (journalID == (byte)0)
      {
         journalToUse = bindingsJournal;
      }
      else
      {
         journalToUse = messagingJournal;
      }
      return journalToUse;
   }

   // Inner classes -------------------------------------------------

}
