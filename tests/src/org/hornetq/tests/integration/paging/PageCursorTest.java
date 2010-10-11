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

import java.util.HashMap;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.impl.PageCursorProviderImpl;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.paging.impl.PagingStoreImpl;
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

      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProviderImpl cursorProvider = new PageCursorProviderImpl(lookupPageStore(ADDRESS), server.getStorageManager(), server.getExecutorFactory());
      
      PageCursor cursor = cursorProvider.createCursor();
      
      Pair<PagePosition, ServerMessage> msg;
      
      int key = 0;
      while ((msg = cursor.moveNext()) != null)
      {
         assertEquals(key++, msg.b.getIntProperty("key").intValue());
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
      
      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);
      
      PageCursor cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getCursor(queue.getID());
      
      System.out.println("Cursor: " + cursor);
      for (int i = 0 ; i < 1000 ; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
         
         if (i < 500)
         {
            cursor.ack(msg.a);
         }
      }
      
      OperationContextImpl.getContext(null).waitCompletion();
      
      server.stop();
      
      server.start();
      
      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getCursor(queue.getID());
      
      for (int i = 500; i < NUM_MESSAGES; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
      
      
      
   }
   
   
   public void testRestartWithHoleOnAck() throws Exception
   {

      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 10 * 1024);
      
      System.out.println("Number of pages = " + numberOfPages);
      
      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);
      
      PageCursor cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getCursor(queue.getID());
      
      System.out.println("Cursor: " + cursor);
      for (int i = 0 ; i < 100 ; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ack(msg.a);
         }
      }
      
      server.stop();
      
      server.start();
      
      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getCursor(queue.getID());
      
      for (int i = 10; i <= 20; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
    
      
      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
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
      
      PageCursor cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getCursor(queue.getID());
      
      System.out.println("Cursor: " + cursor);
      
      Transaction tx = new TransactionImpl(server.getStorageManager(), 60 * 1000);
      for (int i = 0 ; i < 100 ; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ackTx(tx, msg.a);
         }
      }
      
      tx.commit();
      
      server.stop();
      
      server.start();
      
      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getCursor(queue.getID());
      
      for (int i = 10; i <= 20; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
    
      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         Pair<PagePosition, ServerMessage> msg =  cursor.moveNext();
         assertEquals(i, msg.b.getIntProperty("key").intValue());
         cursor.ack(msg.a);
      }
      
   }
   
   
   public void testRollbackScenariosOnACK() throws Exception
   {
      
   }
   
   public void testReadRolledBackData() throws Exception
   {
      
   }
   
   public void testPrepareScenarios() throws Exception
   {
      
   }
   
   public void testRedeliveryScenarios() throws Exception
   {
      
   }
   
   public void testCleanupScenarios() throws Exception
   {
      // Validate the pages are being cleared (with multiple cursors)
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

   /**
    * @param numMessages
    * @param pageStore
    * @throws Exception
    */
   private int addMessages(final int numMessages, final int messageSize) throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      for (int i = 0; i < numMessages; i++)
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
