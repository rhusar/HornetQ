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
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.hornetq.api.core.Pair;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursor;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ConcurrentSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.Future;
import org.hornetq.utils.SoftValueHashMap;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 * A PageProviderIMpl
 * 
 * TODO: this may be moved entirely into PagingStore as there's an one-to-one relationship here
 *       However I want to keep this isolated as much as possible during development
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 *
 */
public class PageCursorProviderImpl implements PageCursorProvider
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PageCursorProviderImpl.class);

   // Attributes ----------------------------------------------------

   private final PagingStore pagingStore;

   private final PagingManager pagingManager;

   private final StorageManager storageManager;

   private final ExecutorFactory executorFactory;

   private final Executor executor;

   private SoftValueHashMap<Long, PageCache> softCache = new SoftValueHashMap<Long, PageCache>();

   private ConcurrentMap<Long, PageCursor> activeCursors = new ConcurrentHashMap<Long, PageCursor>();

   private ConcurrentSet<PageCursor> nonPersistentCursors = new ConcurrentHashSet<PageCursor>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCursorProviderImpl(final PagingStore pagingStore,
                                 final StorageManager storageManager,
                                 final ExecutorFactory executorFactory)
   {
      this.pagingStore = pagingStore;
      this.pagingManager = pagingStore.getPagingManager();
      this.storageManager = storageManager;
      this.executorFactory = executorFactory;
      this.executor = executorFactory.getExecutor();
    }

   // Public --------------------------------------------------------

   public PagingStore getAssociatedStore()
   {
      return pagingStore;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursorProvider#createCursor()
    */
   public PageCursor getPersistentCursor(long cursorID)
   {
      PageCursor activeCursor = activeCursors.get(cursorID);
      if (activeCursor == null)
      {
         activeCursor = new PageCursorImpl(this, pagingStore, storageManager, executorFactory.getExecutor(), cursorID);
         PageCursor previousValue = activeCursors.putIfAbsent(cursorID, activeCursor);
         if (previousValue != null)
         {
            activeCursor = previousValue;
         }
      }

      return activeCursor;
   }

   /**
    * this will create a non-persistent cursor
    */
   public PageCursor createNonPersistentCursor()
   {
      PageCursor cursor = new PageCursorImpl(this, pagingStore, storageManager, executorFactory.getExecutor(), 0);
      nonPersistentCursors.add(cursor);
      return cursor;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursorProvider#getAfter(org.hornetq.core.paging.cursor.PagePosition)
    */
   public Pair<PagePosition, PagedMessage> getNext(final PageCursor cursor, PagePosition cursorPos) throws Exception
   {

      while (true)
      {
         Pair<PagePosition, PagedMessage> retPos = internalAfter(cursorPos);

         if (retPos == null)
         {
            return null;
         }
         else if (retPos != null)
         {
            cursorPos = retPos.a;
            if (retPos.b.getTransactionID() != 0)
            {
               PageTransactionInfo tx = pagingManager.getTransaction(retPos.b.getTransactionID());
               if (tx == null)
               {
                  log.warn("Couldn't locate page transaction " + retPos.b.getTransactionID() +
                           ", ignoring message on position " +
                           retPos.a);
                  cursor.positionIgnored(cursorPos);
               }
               else
               {
                  if (!tx.deliverAfterCommit(cursor, cursorPos))
                  {
                     return retPos;
                  }
               }
            }
            else
            {
               return retPos;
            }
         }
      }
   }

   private Pair<PagePosition, PagedMessage> internalAfter(final PagePosition pos)
   {
      PagePosition retPos = pos.nextMessage();

      PageCache cache = getPageCache(pos);

      if (!cache.isLive() && retPos.getMessageNr() >= cache.getNumberOfMessages())
      {
         retPos = pos.nextPage();

         cache = getPageCache(retPos);

         if (cache == null)
         {
            return null;
         }

         if (retPos.getMessageNr() >= cache.getNumberOfMessages())
         {
            return null;
         }
      }

      PagedMessage serverMessage = cache.getMessage(retPos.getMessageNr());

      if (serverMessage != null)
      {
         return new Pair<PagePosition, PagedMessage>(retPos, cache.getMessage(retPos.getMessageNr()));
      }
      else
      {
         return null;
      }
   }

   public PagedMessage getMessage(final PagePosition pos) throws Exception
   {
      PageCache cache = getPageCache(pos);

      if (pos.getMessageNr() >= cache.getNumberOfMessages())
      {
         // sanity check, this should never happen unless there's a bug
         throw new IllegalStateException("Invalid messageNumber passed = " + pos);
      }

      return cache.getMessage(pos.getMessageNr());
   }

   /**
    * No need to synchronize this method since the private getPageCache will have a synchronized call
    */
   public PageCache getPageCache(PagePosition pos)
   {
      PageCache cache = pos.getPageCache();
      if (cache == null)
      {
         cache = getPageCache(pos.getPageNr());
         pos.setPageCache(cache);
      }
      return cache;
   }

   public synchronized void addPageCache(PageCache cache)
   {
      softCache.put(cache.getPageId(), cache);
   }

   public synchronized int getCacheSize()
   {
      return softCache.size();
   }

   public void processReload() throws Exception
   {
      for (PageCursor cursor : this.activeCursors.values())
      {
         cursor.processReload();
      }
   }

   public void stop()
   {
      for (PageCursor cursor : activeCursors.values())
      {
         cursor.stop();
      }

      for (PageCursor cursor : nonPersistentCursors)
      {
         cursor.stop();
      }

      Future future = new Future();

      executor.execute(future);

      while (!future.await(10000))
      {
         log.warn("Waiting cursor provider " + this + " to finish executors");
      }

   }

   public void close(PageCursor cursor)
   {
      if (cursor.getId() != 0)
      {
         activeCursors.remove(cursor.getId());
      }
      else
      {
         nonPersistentCursors.remove(cursor);
      }

      scheduleCleanup();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursorProvider#scheduleCleanup()
    */
   public void scheduleCleanup()
   {

      executor.execute(new Runnable()
      {
         public void run()
         {
            cleanup();
         }
      });
   }

   private void cleanup()
   {
      long minPage = getMinPageInUse();

      try
      {
         for (long i = pagingStore.getFirstPage(); i < minPage; i++)
         {
            Page page = pagingStore.depage();
            if (page != null)
            {
               page.delete();
            }
         }
      }
      catch (Exception ex)
      {
         log.warn("Couldn't complete cleanup on paging", ex);
      }
   }

   public void printDebug()
   {
      for (PageCache cache : softCache.values())
      {
         System.out.println("Cache " + cache);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /* Protected as we may let test cases to instrument the test */
   protected PageCacheImpl createPageCache(final long pageId) throws Exception
   {
      return new PageCacheImpl(pagingStore.createPage((int)pageId));
   }

   // Private -------------------------------------------------------

   private long getMinPageInUse()
   {
      ArrayList<PageCursor> cursorList = new ArrayList<PageCursor>();
      synchronized (this)
      {
         cursorList.addAll(activeCursors.values());
         cursorList.addAll(nonPersistentCursors);
      }

      if (cursorList.size() == 0)
      {
         return 0l;
      }

      long minPage = Long.MAX_VALUE;

      for (PageCursor cursor : cursorList)
      {
         long firstPage = cursor.getFirstPage();
         if (firstPage < minPage)
         {
            minPage = firstPage;
         }
      }

      return minPage;

   }

   private PageCache getPageCache(final long pageId)
   {
      try
      {
         boolean needToRead = false;
         PageCache cache = null;
         synchronized (this)
         {
            if (pageId > pagingStore.getCurrentWritingPage())
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

               int i = 0;
               for (PagedMessage pdgMessage : pgdMessages)
               {
                  pdgMessage.initMessage(storageManager);
               }

               cache.setMessages(pgdMessages.toArray(new PagedMessage[pgdMessages.size()]));

            }
            finally
            {
               cache.unlock();
            }
         }

         return cache;
      }
      catch (Exception e)
      {
         throw new RuntimeException("Couldn't complete paging due to an IO Exception on Paging - " + e.getMessage(), e);
      }
   }

   // Inner classes -------------------------------------------------

}
