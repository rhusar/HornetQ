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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.server.ServerMessage;

/**
 * The caching associated to a single page.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PageCacheImpl implements PageCache
{
   
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   
   private ServerMessage[] messages;
   
   private final Page page;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public PageCacheImpl(Page page)
   {
      this.page = page;
   }

   // Public --------------------------------------------------------
   
   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#getPage()
    */
   public Page getPage()
   {
      return page;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#getMessage(int)
    */
   public ServerMessage getMessage(int messageNumber)
   {
      lock.readLock().lock();
      try
      {
         if (messageNumber < messages.length)
         {
            return messages[messageNumber];
         }
         else
         {
            return null;
         }
      }
      finally
      {
         lock.readLock().unlock();
      }
   }
   
   public void lock()
   {
      lock.writeLock().lock();
   }
   
   public void unlock()
   {
      lock.writeLock().unlock();
   }
   
   public void setMessages(ServerMessage[] messages)
   {
      this.messages = messages;
   }
   
   public int getNumberOfMessages()
   {
      lock.readLock().lock();
      try
      {
         return messages.length;
      }
      finally
      {
         lock.readLock().unlock();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#isLive()
    */
   public boolean isLive()
   {
      return false;
   }
   
   public String toString()
   {
      return "PageCacheImpl::page=" + page.getPageId() + " numberOfMessages = " + messages.length;
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
