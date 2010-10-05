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
import java.util.LinkedList;
import java.util.List;

import org.hornetq.api.core.Pair;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.StorageManager;
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
      if (lastPosition == null)
      {
         lastPosition = recoverLastPosition();
      }

      Pair<PagePosition, ServerMessage> message = null;
      do
      {
         message = cursorProvider.getAfter(lastPosition);
         if (message != null)
         {
            lastPosition = message.a;
         }
      }
      while (message != null && !match(message.b));

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
   public void redeliver(final PagePosition position)
   {
      // TODO Auto-generated method stub

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

   public void processReload()
   {
      if (this.recoveredACK != null)
      {
         Collections.sort(recoveredACK);
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


   private PagePosition recoverLastPosition()
   {
      long firstPage = pageStore.getFirstPage();
      return new PagePositionImpl(firstPage, -1);
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
