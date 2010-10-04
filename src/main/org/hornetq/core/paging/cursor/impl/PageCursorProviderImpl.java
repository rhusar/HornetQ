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

import java.util.List;

import org.hornetq.api.core.Pair;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.SoftValueHashMap;

/**
 * A PageProviderIMpl
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 *
 */
public class PageCursorProviderImpl implements PageCursorProvider
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final PagingStore pagingStore;
   
   private final StorageManager storageManager;
   
   private SoftValueHashMap<Long, PageCacheImpl> softCache = new SoftValueHashMap<Long, PageCacheImpl>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCursorProviderImpl(final PagingStore pagingStore, final StorageManager storageManager)
   {
      this.pagingStore = pagingStore;
      this.storageManager = storageManager;
   }

   // Public --------------------------------------------------------

   public PagingStore getAssociatedStore()
   {
      return pagingStore;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursorProvider#createCursor()
    */
   public PageCursor createCursor()
   {
      return new PageCursorImpl(this, pagingStore, null);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursorProvider#recoverCursor(org.hornetq.core.paging.cursor.PagePosition)
    */
   public PageCursor recoverCursor(final PagePositionImpl position)
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursorProvider#getAfter(org.hornetq.core.paging.cursor.PagePosition)
    */
   public Pair<PagePosition, ServerMessage> getAfter(final PagePosition pos) throws Exception
   {
      PagePosition retPos = pos.nextMessage();
      
      PageCache cache = getPageCache(pos.getPageNr());
      
      if (retPos.getMessageNr() >= cache.getNumberOfMessages())
      {
         retPos = pos.nextPage();
         
         cache = getPageCache(retPos.getPageNr());
         if (cache == null)
         {
            return null;
         }
         
         if (retPos.getMessageNr() >= cache.getNumberOfMessages())
         {
            return null;
         }
      }
      
      return new Pair<PagePosition, ServerMessage>(retPos, cache.getMessage(retPos.getMessageNr()));
   }

   public PageCache getPageCache(final long pageId) throws Exception
   {
      boolean needToRead = false;
      PageCacheImpl cache = null;
      synchronized (this)
      {
         if (pageId > pagingStore.getNumberOfPages())
         {
            return null;
         }
         
         cache = softCache.get(pageId);
         if (cache == null)
         {
            cache = createPageCache(pageId);
            needToRead = true;
            // anyone reading from this cache will have to wait reading to finish first
            // we also want only one thread reading this cache
            cache.lock();
            softCache.put(pageId, cache);
         }
      }
      
      // Reading is done outside of the synchronized block, however
      // the page stays locked until the entire reading is finished
      if (needToRead)
      {
         try
         {
            Page page = pagingStore.createPage((int)pageId);
            
            page.open();
            
            List<PagedMessage> pgdMessages = page.read();
            
            ServerMessage srvMessages[] = new ServerMessage[pgdMessages.size()];
            
            int i = 0;
            for (PagedMessage pdgMessage : pgdMessages)
            {
               ServerMessage message = pdgMessage.getMessage(storageManager);
               srvMessages[i++] = message;
            }
            
            cache.setMessages(srvMessages);
            
         }
         finally
         {
            cache.unlock();
         }
      }
      
      
      return cache;
   }
   
   public int getCacheSize()
   {
      return softCache.size();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected PageCacheImpl createPageCache(final long pageId) throws Exception
   {
      return new PageCacheImpl(pagingStore.createPage((int)pageId));
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
