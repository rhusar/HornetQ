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

import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hornetq.api.core.Pair;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;

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

   // Attributes ----------------------------------------------------

   private final StorageManager store;

   private final long cursorId;

   private final PagingStore pageStore;

   private final PageCursorProvider cursorProvider;

   private volatile PagePosition lastPosition;

   private List<PagePosition> recoveredACK;

   // We only store the position for redeliveries. They will be read from the SoftCache again during delivery.
   private final ConcurrentLinkedQueue<PagePosition> redeliveries = new ConcurrentLinkedQueue<PagePosition>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCursorImpl(final PageCursorProvider cursorProvider,
                         final PagingStore pageStore,
                         final StorageManager store,
                         final long cursorId)
   {
      this.pageStore = pageStore;
      this.store = store;
      this.cursorProvider = cursorProvider;
      this.cursorId = cursorId;
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
         }
         match = match(message.b);

         if (!match)
         {
            ignored(message.a);
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
      store.storeCursorAcknowledge(cursorId, position);
   }

   public void ackTx(final Transaction tx, final PagePosition position) throws Exception
   {
      store.storeCursorAcknowledgeTransactional(tx.getID(), cursorId, position);
      installTXCallback(tx, position);
      // tx.afterCommit()
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#returnElement(org.hornetq.core.paging.cursor.PagePosition)
    */
   public synchronized void redeliver(final PagePosition position)
   {
      this.redeliveries.add(position);
   }

   /** 
    * Theres no need to synchronize this method as it's only called from journal load on startup
    */
   public void reloadACK(final PagePosition position)
   {
      internalAdd(position);

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#recoverPreparedACK(org.hornetq.core.paging.cursor.PagePosition)
    */
   public void reloadPreparedACK(final Transaction tx, final PagePosition position)
   {
      internalAdd(position);
      installTXCallback(tx, position);
   }

   public void processReload() throws Exception
   {
      if (this.recoveredACK != null)
      {
         System.out.println("********** processing reload!!!!!!!");
         Collections.sort(recoveredACK);

         PagePosition previousPos = null;
         for (PagePosition pos : recoveredACK)
         {
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
                     // end of the hole, we can finish processing here
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean match(final ServerMessage message)
   {
      // To be used with expressions
      return true;
   }

   // Private -------------------------------------------------------

   private void ignored(final PagePosition message)
   {
      // TODO: Update reference counts
   }

   /**
    * @param committedACK
    */
   private void internalAdd(final PagePosition committedACK)
   {
      if (recoveredACK == null)
      {
         recoveredACK = new LinkedList<PagePosition>();
      }

      recoveredACK.add(committedACK);
   }

   /**
    * @param tx
    * @param position
    */
   private void installTXCallback(Transaction tx, PagePosition position)
   {
   }

   // Inner classes -------------------------------------------------

}
