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
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PagedReferenceImpl;
import org.hornetq.core.paging.cursor.impl.PageCursorProviderImpl;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.paging.cursor.impl.PageSubscriptionImpl;
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
import org.hornetq.utils.LinkedListIterator;

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

      PageCursorProviderImpl cursorProvider = new PageCursorProviderImpl(lookupPageStore(ADDRESS),
                                                                         server.getStorageManager(),
                                                                         server.getExecutorFactory());

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

      PageSubscription cursor = lookupPageStore(ADDRESS).getCursorProvier().getSubscription(queue.getID());

      PagedReferenceImpl msg;

      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();
      int key = 0;
      while ((msg = iterator.next()) != null)
      {
         assertEquals(key++, msg.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg.getPosition());
      }
      assertEquals(NUM_MESSAGES, key);

      server.getStorageManager().waitOnOperations();

      waitCleanup();

      assertFalse(lookupPageStore(ADDRESS).isPaging());

      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

      forceGC();

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testSimpleCursorWithFilter() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageSubscription cursorEven = createNonPersistentCursor(new Filter()
      {

         public boolean match(ServerMessage message)
         {
            Boolean property = message.getBooleanProperty("even");
            if (property == null)
            {
               return false;
            }
            else
            {
               return property.booleanValue();
            }
         }

         public SimpleString getFilterString()
         {
            return new SimpleString("even=true");
         }

      });

      PageSubscription cursorOdd = createNonPersistentCursor(new Filter()
      {

         public boolean match(ServerMessage message)
         {
            Boolean property = message.getBooleanProperty("even");
            if (property == null)
            {
               return false;
            }
            else
            {
               return !property.booleanValue();
            }
         }

         public SimpleString getFilterString()
         {
            return new SimpleString("even=true");
         }

      });

      queue.getPageSubscription().close();

      PagedReferenceImpl msg;

      LinkedListIterator<PagedReferenceImpl> iteratorEven = cursorEven.iterator();

      LinkedListIterator<PagedReferenceImpl> iteratorOdd = cursorOdd.iterator();

      int key = 0;
      while ((msg = iteratorEven.next()) != null)
      {
         System.out.println("Received" + msg);
         assertEquals(key, msg.getMessage().getIntProperty("key").intValue());
         assertTrue(msg.getMessage().getBooleanProperty("even").booleanValue());
         key += 2;
         cursorEven.ack(msg.getPosition());
      }
      assertEquals(NUM_MESSAGES, key);

      key = 1;
      while ((msg = iteratorOdd.next()) != null)
      {
         assertEquals(key, msg.getMessage().getIntProperty("key").intValue());
         assertFalse(msg.getMessage().getBooleanProperty("even").booleanValue());
         key += 2;
         cursorOdd.ack(msg.getPosition());
      }
      assertEquals(NUM_MESSAGES + 1, key);

      forceGC();

      // assertTrue(lookupCursorProvider().getCacheSize() < numberOfPages);

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testReadNextPage() throws Exception
   {

      final int NUM_MESSAGES = 1;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(2, 0));

      assertNull(cache);
   }

   public void testRestart() throws Exception
   {
      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 100 * 1024);

      System.out.println("Number of pages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvier()
                                           .getSubscription(queue.getID());

      PageCache firstPage = cursorProvider.getPageCache(new PagePositionImpl(server.getPagingManager()
                                                                                   .getPageStore(ADDRESS)
                                                                                   .getFirstPage(), 0));

      int firstPageSize = firstPage.getNumberOfMessages();

      firstPage = null;

      System.out.println("Cursor: " + cursor);
      cursorProvider.printDebug();

      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();

      for (int i = 0; i < 1000; i++)
      {
         System.out.println("Reading Msg : " + i);
         PagedReferenceImpl msg = iterator.next();
         assertNotNull(msg);
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());

         if (i < firstPageSize)
         {
            cursor.ack(msg);
         }
      }
      cursorProvider.printDebug();

      server.getStorageManager().waitOnOperations();
      lookupPageStore(ADDRESS).flushExecutors();

      // needs to clear the context since we are using the same thread over two distinct servers
      // otherwise we will get the old executor on the factory
      OperationContextImpl.clearContext();

      server.stop();

      server.start();

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getSubscription(queue.getID());

      iterator = cursor.iterator();

      for (int i = firstPageSize; i < NUM_MESSAGES; i++)
      {
         System.out.println("Received " + i);
         PagedReferenceImpl msg = iterator.next();
         assertNotNull(msg);
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());

         cursor.ack(msg);

         OperationContextImpl.getContext(null).waitCompletion();

      }

      OperationContextImpl.getContext(null).waitCompletion();

      lookupPageStore(ADDRESS).flushExecutors();

      assertFalse(lookupPageStore(ADDRESS).isPaging());

      server.stop();
      createServer();
      assertFalse(lookupPageStore(ADDRESS).isPaging());
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testRestartWithHoleOnAck() throws Exception
   {

      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 10 * 1024);

      System.out.println("Number of pages = " + numberOfPages);

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvier()
                                           .getSubscription(queue.getID());

      System.out.println("Cursor: " + cursor);
      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();
      for (int i = 0; i < 100; i++)
      {
         PagedReferenceImpl msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ack(msg);
         }
      }

      server.stop();

      OperationContextImpl.clearContext();

      server.start();

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getSubscription(queue.getID());
      iterator = cursor.iterator();

      for (int i = 10; i <= 20; i++)
      {
         PagedReferenceImpl msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg);
      }

      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         PagedReferenceImpl msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg);
      }

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testRestartWithHoleOnAckAndTransaction() throws Exception
   {
      final int NUM_MESSAGES = 1000;

      int numberOfPages = addMessages(NUM_MESSAGES, 10 * 1024);

      System.out.println("Number of pages = " + numberOfPages);

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvier()
                                           .getSubscription(queue.getID());

      System.out.println("Cursor: " + cursor);

      Transaction tx = new TransactionImpl(server.getStorageManager(), 60 * 1000);

      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();

      for (int i = 0; i < 100; i++)
      {
         PagedReferenceImpl msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         if (i < 10 || i > 20)
         {
            cursor.ackTx(tx, msg);
         }
      }

      tx.commit();

      server.stop();

      OperationContextImpl.clearContext();

      server.start();

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getSubscription(queue.getID());

      tx = new TransactionImpl(server.getStorageManager(), 60 * 1000);
      iterator = cursor.iterator();

      for (int i = 10; i <= 20; i++)
      {
         PagedReferenceImpl msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ackTx(tx, msg);
      }

      for (int i = 100; i < NUM_MESSAGES; i++)
      {
         PagedReferenceImpl msg = iterator.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
         cursor.ackTx(tx, msg);
      }

      tx.commit();

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testConsumeLivePage() throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_MESSAGES = 100;

      final int messageSize = 1024 * 1024;

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvier()
                                           .getSubscription(queue.getID());

      System.out.println("Cursor: " + cursor);

      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         // if (i % 100 == 0)
         System.out.println("read/written " + i);

         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

         ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
         msg.putIntProperty("key", i);

         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

         Assert.assertTrue(pageStore.page(msg));

         PagedReferenceImpl readMessage = iterator.next();

         assertNotNull(readMessage);

         assertEquals(i, readMessage.getMessage().getIntProperty("key").intValue());

         assertNull(iterator.next());
      }

      server.stop();

      OperationContextImpl.clearContext();

      createServer();

      pageStore = lookupPageStore(ADDRESS);

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getSubscription(queue.getID());
      iterator = cursor.iterator();

      for (int i = 0; i < NUM_MESSAGES * 2; i++)
      {
         if (i % 100 == 0)
            System.out.println("Paged " + i);

         if (i >= NUM_MESSAGES)
         {

            HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

            ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
            msg.putIntProperty("key", i);

            msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

            Assert.assertTrue(pageStore.page(msg));
         }

         PagedReferenceImpl readMessage = iterator.next();

         assertNotNull(readMessage);

         assertEquals(i, readMessage.getMessage().getIntProperty("key").intValue());
      }

      server.stop();

      OperationContextImpl.clearContext();

      createServer();

      pageStore = lookupPageStore(ADDRESS);

      cursor = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier().getSubscription(queue.getID());
      iterator = cursor.iterator();

      for (int i = 0; i < NUM_MESSAGES * 3; i++)
      {
         if (i % 100 == 0)
            System.out.println("Paged " + i);

         if (i >= NUM_MESSAGES * 2 - 1)
         {

            HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

            ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
            msg.putIntProperty("key", i + 1);

            msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

            Assert.assertTrue(pageStore.page(msg));
         }

         PagedReferenceImpl readMessage = iterator.next();

         assertNotNull(readMessage);

         cursor.ack(readMessage);

         assertEquals(i, readMessage.getMessage().getIntProperty("key").intValue());
      }

      PagedReferenceImpl readMessage = iterator.next();

      assertEquals(NUM_MESSAGES * 3, readMessage.getMessage().getIntProperty("key").intValue());

      cursor.ack(readMessage);

      server.getStorageManager().waitOnOperations();

      pageStore.flushExecutors();

      assertFalse(pageStore.isPaging());

      server.stop();
      createServer();

      assertFalse(pageStore.isPaging());

      waitCleanup();

      assertFalse(lookupPageStore(ADDRESS).isPaging());

   }

   /**
    * @throws Exception
    * @throws InterruptedException
    */
   private void waitCleanup() throws Exception, InterruptedException
   {
      // The cleanup is done asynchronously, so we need to wait some time
      long timeout = System.currentTimeMillis() + 10000;

      while (System.currentTimeMillis() < timeout && lookupPageStore(ADDRESS).getNumberOfPages() != 1)
      {
         Thread.sleep(100);
      }

      assertTrue("expected " + lookupPageStore(ADDRESS).getNumberOfPages(),
                 lookupPageStore(ADDRESS).getNumberOfPages() <= 2);
   }

   public void testPrepareScenarios() throws Exception
   {
      PagingStoreImpl pageStore = lookupPageStore(ADDRESS);

      pageStore.startPaging();

      final int NUM_MESSAGES = 100;

      final int messageSize = 100 * 1024;

      PageCursorProvider cursorProvider = this.server.getPagingManager().getPageStore(ADDRESS).getCursorProvier();
      System.out.println("cursorProvider = " + cursorProvider);

      PageSubscription cursor = this.server.getPagingManager()
                                           .getPageStore(ADDRESS)
                                           .getCursorProvier()
                                           .getSubscription(queue.getID());
      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();

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

      System.out.println("Number of pages - " + pageStore.getNumberOfPages());

      // First consume what's already there without any tx as nothing was committed
      for (int i = 300; i < 400; i++)
      {
         PagedReferenceImpl pos = iterator.next();
         assertNotNull("Null at position " + i, pos);
         assertEquals(i, pos.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos);
      }

      assertNull(iterator.next());

      cursor.printDebug();
      pgtxRollback.rollback();

      this.server.getPagingManager().removeTransaction(pgtxRollback.getTransactionID());
      pgtxCommit.commit();

      // Second:after pgtxCommit was done
      for (int i = 200; i < 300; i++)
      {
         PagedReferenceImpl pos = iterator.next();
         assertNotNull(pos);
         assertEquals(i, pos.getMessage().getIntProperty("key").intValue());
         cursor.ack(pos);
      }

      assertNull(iterator.next());

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testCloseNonPersistentConsumer() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageSubscription cursor = cursorProvider.createSubscription(11, null, false);
      PageSubscriptionImpl cursor2 = (PageSubscriptionImpl)cursorProvider.createSubscription(12, null, false);

      queue.getPageSubscription().close();

      PagedReferenceImpl msg;
      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();
      LinkedListIterator<PagedReferenceImpl> iterator2 = cursor.iterator();

      int key = 0;
      while ((msg = iterator.next()) != null)
      {
         assertEquals(key++, msg.getMessage().getIntProperty("key").intValue());
         cursor.ack(msg);
      }
      assertEquals(NUM_MESSAGES, key);

      forceGC();

      for (int i = 0; i < 10; i++)
      {
         msg = iterator2.next();
         assertEquals(i, msg.getMessage().getIntProperty("key").intValue());
      }

      assertSame(cursor2.getProvider(), cursorProvider);

      cursor2.close();

      lookupPageStore(ADDRESS).flushExecutors();

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testNoCursors() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      ClientSessionFactory sf = createInVMFactory();
      ClientSession session = sf.createSession();
      session.deleteQueue(ADDRESS);

      System.out.println("NumberOfPages = " + numberOfPages);

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(0, lookupPageStore(ADDRESS).getNumberOfPages());

   }

   public void testFirstMessageInTheMiddle() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(5, 0));

      PageSubscription cursor = cursorProvider.createSubscription(2, null, false);

      queue.getPageSubscription().close();

      PagePosition startingPos = new PagePositionImpl(5, cache.getNumberOfMessages() / 2);
      cursor.bookmark(startingPos);
      PagedMessage msg = cache.getMessage(startingPos.getMessageNr() + 1);
      msg.initMessage(server.getStorageManager());
      int key = msg.getMessage().getIntProperty("key").intValue();

      msg = null;

      cache = null;
      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();

      PagedReferenceImpl msgCursor = null;
      while ((msgCursor = iterator.next()) != null)
      {
         assertEquals(key++, msgCursor.getMessage().getIntProperty("key").intValue());
         cursor.ack(msgCursor);
      }
      assertEquals(NUM_MESSAGES, key);

      forceGC();

      // assertTrue(cursorProvider.getCacheSize() < numberOfPages);

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());
   }

   public void testFirstMessageInTheMiddlePersistent() throws Exception
   {

      final int NUM_MESSAGES = 100;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageCache cache = cursorProvider.getPageCache(new PagePositionImpl(5, 0));

      PageSubscription cursor = cursorProvider.getSubscription(queue.getID());
      PagePosition startingPos = new PagePositionImpl(5, cache.getNumberOfMessages() / 2);
      cursor.bookmark(startingPos);
      PagedMessage msg = cache.getMessage(startingPos.getMessageNr() + 1);
      msg.initMessage(server.getStorageManager());
      int initialKey = msg.getMessage().getIntProperty("key").intValue();
      int key = initialKey;

      msg = null;

      cache = null;

      LinkedListIterator<PagedReferenceImpl> iterator = cursor.iterator();

      PagedReferenceImpl msgCursor = null;
      while ((msgCursor = iterator.next()) != null)
      {
         assertEquals(key++, msgCursor.getMessage().getIntProperty("key").intValue());
      }
      assertEquals(NUM_MESSAGES, key);

      server.stop();

      OperationContextImpl.clearContext();

      createServer();

      cursorProvider = lookupCursorProvider();
      cursor = cursorProvider.getSubscription(queue.getID());
      key = initialKey;
      iterator = cursor.iterator();
      while ((msgCursor = iterator.next()) != null)
      {
         assertEquals(key++, msgCursor.getMessage().getIntProperty("key").intValue());
         cursor.ack(msgCursor);
      }

      forceGC();

      assertTrue(cursorProvider.getCacheSize() < numberOfPages);

      server.stop();
      createServer();
      waitCleanup();
      assertEquals(1, lookupPageStore(ADDRESS).getNumberOfPages());

   }
   
   private int tstProperty(ServerMessage msg)
   {
      return msg.getIntProperty("key").intValue();
   }

   public void testMultipleIterators() throws Exception
   {

      final int NUM_MESSAGES = 10;

      int numberOfPages = addMessages(NUM_MESSAGES, 1024 * 1024);

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProvider cursorProvider = lookupCursorProvider();

      PageSubscription cursor = cursorProvider.getSubscription(queue.getID());

      Iterator<PagedReferenceImpl> iter = cursor.iterator();
      
      Iterator<PagedReferenceImpl> iter2 = cursor.iterator();
      
      assertTrue(iter.hasNext());
      
      PagedReferenceImpl msg1 = iter.next();
      
      PagedReferenceImpl msg2 = iter2.next();
      
      assertEquals(tstProperty(msg1.getMessage()), tstProperty(msg2.getMessage()));
      
      System.out.println("property = " + tstProperty(msg1.getMessage()));

      msg1 = iter.next();
      
      assertEquals(1, tstProperty(msg1.getMessage()));
      
      iter.remove();
      
      msg2 = iter2.next();
      
      assertEquals(2, tstProperty(msg2.getMessage()));
      
      assertTrue(iter2.hasNext());
      
      
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
         if (i % 100 == 0)
            System.out.println("Paged " + i);
         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);

         ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
         msg.putIntProperty("key", i);
         // to be used on tests that are validating filters
         msg.putBooleanProperty("even", i % 2 == 0);

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

      createServer();
   }

   /**
    * @throws Exception
    */
   private void createServer() throws Exception
   {
      OperationContextImpl.clearContext();

      Configuration config = createDefaultConfig();

      config.setJournalSyncNonTransactional(true);

      server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      try
      {
         queue = server.createQueue(ADDRESS, ADDRESS, null, true, false);
      }
      catch (Exception ignored)
      {
      }
   }

   /**
    * @return
    * @throws Exception
    */
   private PageSubscription createNonPersistentCursor(Filter filter) throws Exception
   {
      return lookupCursorProvider().createSubscription(server.getStorageManager().generateUniqueID(), filter, false);
   }

   /**
    * @return
    * @throws Exception
    */
   private PageCursorProvider lookupCursorProvider() throws Exception
   {
      return lookupPageStore(ADDRESS).getCursorProvier();
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

      for (int i = start; i < start + NUM_MESSAGES; i++)
      {
         HornetQBuffer buffer = RandomUtil.randomBuffer(messageSize, i + 1l);
         ServerMessage msg = new ServerMessageImpl(storage.generateUniqueID(), buffer.writerIndex());
         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());
         msg.putIntProperty("key", i);
         messages.add(msg);
      }

      pageStore.page(messages, pgParameter.getTransactionID());
   }

   protected void tearDown() throws Exception
   {
      server.stop();
      server = null;
      queue = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
