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

import org.hornetq.api.core.Pair;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.ServerMessage;

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

   private StorageManager store;

   private final long cursorId;

   private PagingStore pageStore;

   private final PageCursorProvider cursorProvider;

   private volatile PagePosition lastPosition;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCursorImpl(PageCursorProvider cursorProvider, PagingStore pageStore, StorageManager store, long cursorId)
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
   public void ack(PagePosition position)
   {
      // TODO Auto-generated method stub
   }
   
   public void ack(long tx, PagePosition position)
   {
      
   }
   
   

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#returnElement(org.hornetq.core.paging.cursor.PagePosition)
    */
   public void returnElement(PagePosition position)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#getFirstPosition()
    */
   public PagePosition getFirstPosition()
   {
      // TODO Auto-generated method stub
      return null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean match(ServerMessage message)
   {
      return true;
   }

   // Private -------------------------------------------------------

   private PagePosition recoverLastPosition()
   {
      long firstPage = pageStore.getFirstPage();
      return new PagePositionImpl(firstPage, -1);
   }

   // Inner classes -------------------------------------------------

}
