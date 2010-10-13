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

package org.hornetq.core.paging.cursor.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.Pair;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.TransactionOperationAbstract;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.TransactionImpl;

/**
 * A PageCursorImpl
 *
 * A page cursor will always store its 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 * 
 */
public class PageCursorImpl implements PageCursor
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PageCursorImpl.class);

   // Attributes ----------------------------------------------------

   private final StorageManager store;

   private final long cursorId;

   private final PagingStore pageStore;

   private final PageCursorProvider cursorProvider;

   private final Executor executor;

   private volatile PagePosition lastPosition;

   private List<PagePosition> recoveredACK;

   private final SortedMap<Long, PageCursorInfo> consumedPages = Collections.synchronizedSortedMap(new TreeMap<Long, PageCursorInfo>());

   // We only store the position for redeliveries. They will be read from the SoftCache again during delivery.
   private final ConcurrentLinkedQueue<PagePosition> redeliveries = new ConcurrentLinkedQueue<PagePosition>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCursorImpl(final PageCursorProvider cursorProvider,
                         final PagingStore pageStore,
                         final StorageManager store,
                         final Executor executor,
                         final long cursorId)
   {
      this.pageStore = pageStore;
      this.store = store;
      this.cursorProvider = cursorProvider;
      this.cursorId = cursorId;
      this.executor = executor;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#moveNext()
    */
   public synchronized Pair<PagePosition, ServerMessage> moveNext() throws Exception
   {
      PagePosition redeliveryPos = null;

      // Redeliveries will take precedence
      if ((redeliveryPos = redeliveries.poll()) != null)
      {
         return new Pair<PagePosition, ServerMessage>(redeliveryPos, cursorProvider.getMessage(redeliveryPos));
      }

      if (lastPosition == null)
      {
         // it will start at the first available page
         long firstPage = pageStore.getFirstPage();
         lastPosition = new PagePositionImpl(firstPage, -1);
      }

      boolean match = false;

      Pair<PagePosition, ServerMessage> message = null;

      do
      {
         message = cursorProvider.getAfter(lastPosition);

         if (message != null)
         {
            lastPosition = message.a;

            match = match(message.b);

            if (!match)
            {
               processACK(message.a);
            }
         }

      }
      while (message != null && !match);

      return message;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#confirm(org.hornetq.core.paging.cursor.PagePosition)
    */
   public void ack(final PagePosition position) throws Exception
   {

      // if we are dealing with a persistent cursor
      if (cursorId != 0)
      {
         store.storeCursorAcknowledge(cursorId, position);
      }

      store.afterCompleteOperations(new IOAsyncTask()
      {

         public void onError(final int errorCode, final String errorMessage)
         {
         }

         public void done()
         {
            processACK(position);
         }
      });
   }

   public void ackTx(final Transaction tx, final PagePosition position) throws Exception
   {
      // if the cursor is persistent
      if (cursorId != 0)
      {
         store.storeCursorAcknowledgeTransactional(tx.getID(), cursorId, position);
      }
      installTXCallback(tx, position);

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#returnElement(org.hornetq.core.paging.cursor.PagePosition)
    */
   public synchronized void redeliver(final PagePosition position)
   {
      redeliveries.add(position);
   }

   /** 
    * Theres no need to synchronize this method as it's only called from journal load on startup
    */
   public void reloadACK(final PagePosition position)
   {
      if (recoveredACK == null)
      {
         recoveredACK = new LinkedList<PagePosition>();
      }

      recoveredACK.add(position);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#recoverPreparedACK(org.hornetq.core.paging.cursor.PagePosition)
    */
   public void reloadPreparedACK(final Transaction tx, final PagePosition position)
   {
      // internalAdd(position);
      installTXCallback(tx, position);
   }

   public void processReload() throws Exception
   {
      if (recoveredACK != null)
      {
         System.out.println("********** processing reload!!!!!!!");
         Collections.sort(recoveredACK);

         PagePosition previousPos = null;
         for (PagePosition pos : recoveredACK)
         {
            PageCursorInfo positions = getPageInfo(pos);

            positions.addACK(pos);

            lastPosition = pos;
            if (previousPos != null)
            {
               if (!previousPos.isRightAfter(previousPos))
               {
                  PagePosition tmpPos = previousPos;
                  // looking for holes on the ack list for redelivery
                  while (true)
                  {
                     Pair<PagePosition, ServerMessage> msgCheck = cursorProvider.getAfter(tmpPos);

                     positions = getPageInfo(tmpPos);

                     // end of the hole, we can finish processing here
                     // It may be also that the next was just a next page, so we just ignore it
                     if (msgCheck == null || msgCheck.a.equals(pos))
                     {
                        break;
                     }
                     else
                     {
                        if (match(msgCheck.b))
                        {
                           redeliver(msgCheck.a);
                        }
                        else
                        {
                           // The reference was ignored. But we must take a count from the reference count
                           // otherwise the page will never be deleted hence we would never leave paging even if
                           // everything was consumed
                           positions.confirmed.incrementAndGet();
                        }
                     }
                     tmpPos = msgCheck.a;
                  }
               }
            }

            previousPos = pos;
            System.out.println("pos: " + pos);
         }

         recoveredACK.clear();
         recoveredACK = null;
      }
   }

   /**
    * @param page
    * @return
    */
   private PageCursorInfo getPageInfo(final PagePosition pos)
   {
      PageCursorInfo pageInfo = consumedPages.get(pos.getPageNr());

      if (pageInfo == null)
      {
         PageCache cache = cursorProvider.getPageCache(pos);
         pageInfo = new PageCursorInfo(pos.getPageNr(), cache.getNumberOfMessages());
         consumedPages.put(pos.getPageNr(), pageInfo);
      }

      return pageInfo;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean match(final ServerMessage message)
   {
      // To be used with expressions
      return true;
   }

   // Private -------------------------------------------------------

   // To be called only after the ACK has been processed and guaranteed to be on storae
   // The only exception is on non storage events such as not matching messages
   private void processACK(final PagePosition pos)
   {
      PageCursorInfo info = getPageInfo(pos);

      info.addACK(pos);
   }

   /**
    * @param tx
    * @param position
    */
   private void installTXCallback(final Transaction tx, final PagePosition position)
   {
      if (position.getRecordID() > 0)
      {
         // It needs to persist, otherwise the cursor will return to the fist page position
         tx.setContainsPersistent();
      }

      PageCursorTX cursorTX = (PageCursorTX)tx.getProperty(TransactionPropertyIndexes.PAGE_CURSOR_POSITIONS);

      if (cursorTX == null)
      {
         cursorTX = new PageCursorTX();
         tx.putProperty(TransactionPropertyIndexes.PAGE_CURSOR_POSITIONS, cursorTX);
         tx.addOperation(cursorTX);
      }

      cursorTX.addPositionConfirmation(this, position);

   }

   /**
    *  A callback from the PageCursorInfo. It will be called when all the messages on a page have been acked
    * @param info
    */
   private void onPageDone(final PageCursorInfo info)
   {
      System.out.println("Page " + info.getPageId() + " has completed");

      executor.execute(new Runnable()
      {

         public void run()
         {
            try
            {
               cleanupPages();
            }
            catch (Exception e)
            {
               PageCursorImpl.log.warn("Error on cleaning up cursor pages");
            }
         }
      });
   }

   /** 
    * It will cleanup all the records for completed pages
    * */
   private void cleanupPages() throws Exception
   {
      Transaction tx = new TransactionImpl(store);

      boolean persist = false;

      final ArrayList<PageCursorInfo> completedPages = new ArrayList<PageCursorInfo>();

      // First get the completed pages using a lock   
      synchronized (this)
      {
         for (Entry<Long, PageCursorInfo> entry : consumedPages.entrySet())
         {
            if (entry.getValue().isDone())
            {
               completedPages.add(entry.getValue());
            }
         }
      }

      for (PageCursorInfo info : completedPages)
      {
         for (PagePosition pos : info.acks)
         {
            if (pos.getRecordID() > 0)
            {
               store.deleteCursorAcknowledgeTransactional(tx.getID(), pos.getRecordID());
               if (!persist)
               {
                  // only need to set it once
                  tx.setContainsPersistent();
                  persist = true;
               }
            }
         }
      }

      tx.addOperation(new TransactionOperationAbstract()
      {

         @Override
         public void afterCommit(final Transaction tx)
         {
            synchronized (PageCursorImpl.this)
            {
               for (PageCursorInfo completePage : completedPages)
               {
                  System.out.println("Removing page " + completePage.getPageId());
                  consumedPages.remove(completePage.getPageId());
               }
            }
         }
      });

      tx.commit();

   }

   // Inner classes -------------------------------------------------

   private class PageCursorInfo
   {
      // Number of messages existent on this page
      private final int numberOfMessages;

      private final long pageId;

      // Confirmed ACKs on this page
      private final List<PagePosition> acks = Collections.synchronizedList(new LinkedList<PagePosition>());

      // We need a separate counter as the cursor may be ignoring certain values because of incomplete transactions or
      // expressions
      private final AtomicInteger confirmed = new AtomicInteger(0);

      public PageCursorInfo(final long pageId, final int numberOfMessages)
      {
         this.pageId = pageId;
         this.numberOfMessages = numberOfMessages;
      }

      public boolean isDone()
      {
         return numberOfMessages == confirmed.get();
      }

      /**
       * @return the pageId
       */
      public long getPageId()
      {
         return pageId;
      }

      public void addACK(final PagePosition posACK)
      {
         if (posACK.getRecordID() > 0)
         {
            // We store these elements for later cleanup
            acks.add(posACK);
         }

         if (numberOfMessages == confirmed.incrementAndGet())
         {
            onPageDone(this);
         }
      }

   }

   static class PageCursorTX implements TransactionOperation
   {
      HashMap<PageCursorImpl, List<PagePosition>> pendingPositions = new HashMap<PageCursorImpl, List<PagePosition>>();

      public void addPositionConfirmation(final PageCursorImpl cursor, final PagePosition position)
      {
         List<PagePosition> list = pendingPositions.get(cursor);

         if (list == null)
         {
            list = new LinkedList<PagePosition>();
            pendingPositions.put(cursor, list);
         }

         list.add(position);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#beforePrepare(org.hornetq.core.transaction.Transaction)
       */
      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#afterPrepare(org.hornetq.core.transaction.Transaction)
       */
      public void afterPrepare(final Transaction tx)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#beforeCommit(org.hornetq.core.transaction.Transaction)
       */
      public void beforeCommit(final Transaction tx) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#afterCommit(org.hornetq.core.transaction.Transaction)
       */
      public void afterCommit(final Transaction tx)
      {
         for (Entry<PageCursorImpl, List<PagePosition>> entry : pendingPositions.entrySet())
         {
            PageCursorImpl cursor = entry.getKey();

            List<PagePosition> positions = entry.getValue();

            for (PagePosition confirmed : positions)
            {
               cursor.processACK(confirmed);
            }

         }
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#beforeRollback(org.hornetq.core.transaction.Transaction)
       */
      public void beforeRollback(final Transaction tx) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#afterRollback(org.hornetq.core.transaction.Transaction)
       */
      public void afterRollback(final Transaction tx)
      {
      }
   }

}
