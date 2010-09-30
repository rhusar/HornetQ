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
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.StorageCursor;

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
   
   private StorageCursor store;
   
   private PagingStore pageStore;
   
   public PageCursorImpl(PagingStore pageStore, StorageCursor store)
   {
      this.pageStore = pageStore;
      this.store = store;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#moveNext()
    */
   public Pair<PagePositionImpl, PagedMessage> moveNext()
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#confirm(org.hornetq.core.paging.cursor.PagePosition)
    */
   public void ack(PagePositionImpl position)
   {
      // TODO Auto-generated method stub
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#returnElement(org.hornetq.core.paging.cursor.PagePosition)
    */
   public void returnElement(PagePositionImpl position)
   {
      // TODO Auto-generated method stub
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursor#getFirstPosition()
    */
   public PagePositionImpl getFirstPosition()
   {
      // TODO Auto-generated method stub
      return null;
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
