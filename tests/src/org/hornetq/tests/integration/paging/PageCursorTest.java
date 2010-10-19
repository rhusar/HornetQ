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

package org.hornetq.tests.integration.paging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.impl.PageCursorImpl;
import org.hornetq.core.paging.cursor.impl.PageCursorProviderImpl;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.paging.impl.PagingStoreImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A PageCacheTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PageCursorTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString ADDRESS = new SimpleString("test-add");

   private HornetQServer server;
   
   private Queue queue;

   private static final int PAGE_MAX = -1;

   private static final int PAGE_SIZE = 10 * 1024 * 1024;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Read more cache than what would fit on the memory, and validate if the memory would be cleared through soft-caches
   public void testReadCache() throws Exception
   {

      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProviderImpl cursorProvider = new PageCursorProviderImpl(lookupPageStore(ADDRESS), server.getStorageManager(), server.getExecutorFactory());

      for (int i = 0; i < numberOfPages; i++)
      {
         PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(i + 1, 0));
         System.out.println("Page " + i + " had " + cache.getNumberOfMessages() + " messages");

      }
      
      forceGC();
      
      assertTrue(cursorProvider.getCacheSize() < numberOfPages);
      
      System.out.println("Cache size = " + cursorProvider.getCacheSize());
   }


   public void testSimpleCursor() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProviderImpl cursorProvider = new PageCursorProviderImpl(lookupPageStore(ADDRESS), server.getStorageManager(), server.getExecutorFactory());
      
      PageCursor cursor = cursorProvider.createNonPersistentCursor();
      
      Pair<PagePosition, PagedMessage> msg;
      
      int key = 0;
      while ((msg = cursor.moveNext()) != null)
      {
         assertEquals(key++, msg.b.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
      assertEquals(NUM_MESSAGES, key);
      
      
      forceGC();
      
      assertTrue(cursorProvider.getCacheSize() < numberOfPages);

   }


   public void testReadNextPage() throws Exception
   {

      final int NUM_MESSAGES = 1;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProviderImpl cursorProvider = new PageCursorProviderImpl(lookupPageStore(ADDRESS), server.getStorageManager(), server.getExecutorFactory());
      
      PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(2,0));
      
      assertNull(cache);
   }
   
   
   public void testRestart() throws Exception
   {
      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 100 * 1024);
      
      System.out.println("Number of pages = " + numberOfPages);
      
      PageCursorProviderImpl cursorProvider = (PageCursorProviderImpl)this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      
      
      PageCursor cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getPersistentCursor(queue.getID());
 
      PageCache firstPage = cursorProvider.getPageCache(new PagePositionImpl(server.getPagingManager().getPageStore(ADDRESS).getFirstPage(), 0));

      int firstPageSize = firstPage.getNumberOfMessages();
      
      firstPage = null;
      
      System.out.println("Cursor: " + cursor);
      cursorProvider.printDebug();

      for (int i = 0 ; i < 1000 ; i++)
      {
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertNotNull(msg);
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         
         if (i < firstPageSize)
         {
            cursor.ack(msg.a);
         }
      }
      cursorProvider.printDebug();
     
      // needs to clear the context since we are using the same thread over two distinct servers
      // otherwise we will get the old executor on the factory
      OperationContextImpl.clearContext();
      
      server.stop();
      
      server.start();
      
      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getPersistentCursor(queue.getID());
      
      for (int i = firstPageSize; i < NUM_MESSAGES; i++)
      {
         System.out.println("Received " + i);
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertNotNull(msg);
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         
         cursor.ack(msg.a);
         
         OperationContextImpl.getContext(null).waitCompletion();
         
      }

      OperationContextImpl.getContext(null).waitCompletion(); 
      ((PageCursorImpl)cursor).printDebug();
      
      
   }
   
   public void testRestartWithHoleOnAck() throws Exception
   {

      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 10 * 1024);
      
      System.out.println("Number of pages = " + numberOfPages);
      
      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);
      
      PageCursor cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getPersistentCursor(queue.getID());
      
      System.out.println("Cursor: " + cursor);
      for (int i = 0 ; i < 100 ; i++)
      {
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ack(msg.a);
         }
      }
      
      server.stop();
      
      OperationContextImpl.clearContext();
      
      server.start();
      
      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getPersistentCursor(queue.getID());
      
      for (int i = 10; i <= 20; i++)
      {
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
    
      
      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
      
   }
   
   
   public void testRestartWithHoleOnAckAndTransaction() throws Exception
   {
      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 10 * 1024);
      
      System.out.println("Number of pages = " + numberOfPages);
      
      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);
      
      PageCursor cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getPersistentCursor(queue.getID());
      
      System.out.println("Cursor: " + cursor);
      
      Transaction tx = new TransactionImpl(server.getStorageManager(), 60 * 1000);
      for (int i = 0 ; i < 100 ; i++)
      {
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ackTx(tx, msg.a);
         }
      }
      
      tx.commit();
      
      server.stop();
      
      OperationContextImpl.clearContext();
      
      server.start();
      
      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getPersistentCursor(queue.getID());

      tx = new TransactionImpl(server.getStorageManager(), 60 * 1000);
      
      for (int i = 10; i <= 20; i++)
      {
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         cursor.ackTx(tx,msg.a);
      }
    
      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         Pair<PagePosition, PagedMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
         cursor.ackTx(tx,msg.a);
      }
      
      tx.commit();
      
   }
   
   public void testConsumeLivePage() throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_MESSAGES = 1000;
      
      final int messageSize = 1024 * 1024;
      
      
      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);
      
      PageCursor cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getPersistentCursor(queue.getID());
      
      System.out.println("Cursor: " + cursor);

      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         if (i % 100 == 0) System.out.println("Paged " + i);
         
         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

         ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
         msg.putIntProperty("key", i);
         
         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

         Assert.assertTrue(pageStore.page(msg));
         
         Pair<PagePosition, PagedMessage> readMessage = cursor.moveNext();
         
         assertNotNull(readMessage);
         
         cursor.ack(readMessage.a);
         
         assertEquals(i, readMessage.b.getMessage().getIntProperty("key").intValue());
         
         assertNull(cursor.moveNext());
      }
      
   }
   
   
   public void testPrepareScenarios() throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_MESSAGES = 100;
      
      final int messageSize = 10 * 1024;
      
      
      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);
       
      PageCursor cursor = pageStore.getCursorProvier().getPersistentCursor(queue.getID());
      
      System.out.println("Cursor: " + cursor);
      
      StorageManager storage = this.server.getStorageManager();
      
      PageTransactionInfoImpl pgtxRollback = new PageTransactionInfoImpl(storage.generateUniqueID());
      PageTransactionInfoImpl pgtxForgotten = new PageTransactionInfoImpl(storage.generateUniqueID());
      PageTransactionInfoImpl pgtxCommit = new PageTransactionInfoImpl(storage.generateUniqueID());
      
      System.out.println("Forgetting tx " + pgtxForgotten.getTransactionID());
      
      this.server.getPagingManager().addTransaction(pgtxRollback);
      this.server.getPagingManager().addTransaction(pgtxCommit);
      
      pgMessages(storage, pageStore, pgtxRollback, 0, NUM_MESSAGES, messageSize);
      pageStore.forceAnotherPage();
      pgMessages(storage, pageStore, pgtxForgotten, 100, NUM_MESSAGES, messageSize);
      pageStore.forceAnotherPage();
      pgMessages(storage, pageStore, pgtxCommit, 200, NUM_MESSAGES, messageSize);
      pageStore.forceAnotherPage();
      
      addMessages(300, NUM_MESSAGES, messageSize);
      pageStore.forceAnotherPage();


      // First consume what's already there without any tx as nothing was committed
      for (int i = 300; i < 400; i++)
      {
         Pair<PagePosition, PagedMessage> pos = cursor.moveNext();
         assertNotNull("Null at position " + i, pos);
         assertEquals(i, pos.b.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos.a);
      }

      assertNull(cursor.moveNext());
      
      pgtxRollback.rollback();
      this.server.getPagingManager().removeTransaction(pgtxRollback.getTransactionID());
      pgtxCommit.commit();
      // Second:after pgtxCommit was done
      for (int i = 200; i < 300; i++)
      {
         Pair<PagePosition, PagedMessage> pos = cursor.moveNext();
         assertNotNull(pos);
         assertEquals(i, pos.b.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos.a);
      }
      
      
   }


   /**
    * @param storage
    * @param pageStore
    * @param pgParameter
    * @param start
    * @param NUM_MESSAGES
    * @param messageSize
    * @throws Exception
    */
   private void pgMessages(StorageManager storage,
                           PagingStoreImpl pageStore,
                           PageTransactionInfo pgParameter,
                           int start,
                           final int NUM_MESSAGES,
                           final int messageSize) throws Exception
   {
      List<ServerMessage> messages = new ArrayList<ServerMessage>();
      
      for (int i = start ; i < start + NUM_MESSAGES; i++)
      {
         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);
         ServerMessage msg = new ServerMessageImpl(storage.generateUniqueID(), buffer.writerIndex());
         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());
         msg.putIntProperty("key", i);
         messages.add(msg);
      }
      
      pageStore.page(messages, pgParameter.getTransactionID());
   }
   
   public void testCleanupScenarios() throws Exception
   {
      // Validate the pages are being cleared (with multiple cursors)
   }
   
   
   public void testCloseNonPersistentConsumer() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupPageStore(ADDRESS).getCursorProvier();
      
      PageCursor cursor = cursorProvider.createNonPersistentCursor();
      PageCursorImpl cursor2 = (PageCursorImpl)cursorProvider.createNonPersistentCursor();
      
      Pair<PagePosition, PagedMessage> msg;
      
      int key = 0;
      while ((msg = cursor.moveNext()) != null)
      {
         assertEquals(key++, msg.b.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
      assertEquals(NUM_MESSAGES, key);
      
      
      forceGC();
      
      assertTrue(cursorProvider.getCacheSize() < numberOfPages);
      
      for (int i = 0 ; i < 10; i++)
      {
         msg = cursor2.moveNext();
         assertEquals(i, msg.b.getMessage().getIntProperty("key").intValue());
      }
        
      assertSame(cursor2.getProvider(), cursorProvider);

      cursor2.close();
      
      server.stop();

   }
  
   
   public void testLeavePageStateAndRestart() throws Exception
   {
      // Validate the cursor are working fine when all the pages are gone, and then paging being restarted   
   }
   
   public void testRedeliveryWithCleanup() throws Exception
   {
      
   }
   
   public void testFirstMessageInTheMiddle() throws Exception
   {
      
   }
   
   private int addMessages(final int numMessages, final int messageSize) throws Exception
   {
      return addMessages(0, numMessages, messageSize);
   }

   /**
    * @param numMessages
    * @param pageStore
    * @throws Exception
    */
   private int addMessages(final int start, final int numMessages, final int messageSize) throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      for (int i = start; i < start + numMessages; i++)
      {
         if (i % 100 == 0) System.out.println("Paged " + i);
         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

         ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
         msg.putIntProperty("key", i);
         
         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

         Assert.assertTrue(pageStore.page(msg));
      }
      
      return pageStore.getNumberOfPages();
   }

   /**
    * @return
    * @throws Exception
    */
   private PagingStoreImpl lookupPageStore(SimpleString address) throws Exception
   {
      return (PagingStoreImpl)server.getPagingManager().getPageStore(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      OperationContextImpl.clearContext();
      System.out.println("Tmp:" + getTemporaryDir());
      
      Configuration config = createDefaultConfig();
      
      config.setJournalSyncNonTransactional(true);

      server = createServer(true,
                            config,
                            PAGE_SIZE,
                            PAGE_MAX,
                            new HashMap<String, AddressSettings>());

      server.start();
      
      queue = server.createQueue(ADDRESS, ADDRESS, null, true, false);

      //createQueue(ADDRESS.toString(), ADDRESS.toString());
   }

   protected void tearDown() throws Exception
   {
      server.stop();
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
