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
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.impl.PageCursorProviderImpl;
import org.hornetq.core.paging.impl.PagingStoreImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A PageCacheTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PageCacheTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString ADDRESS = new SimpleString("test-add");

   private HornetQServer server;

   private static final int PAGE_MAX = -1;

   private static final int PAGE_SIZE = 10 * 1024 * 1024;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReadCache() throws Exception
   {

      PagingStoreImpl pageStore = (PagingStoreImpl)server.getPagingManager().getPageStore(ADDRESS);

      StorageManager storageManager = server.getStorageManager();

      final int NUM_MESSAGES = 1000;

      pageStore.startPaging();

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         if (i % 100 == 0) System.out.println("Paged " + i);
         HornetQBuffer buffer = RandomUtil.randomBuffer(1024*1024, i + 1l);

         ServerMessage msg = new ServerMessageImpl(i, buffer.writerIndex());
         msg.getBodyBuffer().writeBytes(buffer, 0, buffer.writerIndex());

         Assert.assertTrue(pageStore.page(msg));
      }

      int numberOfPages = pageStore.getNumberOfPages();

      System.out.println("NumberOfPages = " + numberOfPages);

      PageCursorProviderImpl cursorProvider = new PageCursorProviderImpl(pageStore, storageManager);

      for (int i = 0; i < numberOfPages; i++)
      {
         PageCache cache = cursorProvider.getPageCache(i + 1);
         System.out.println("Page " + i + " had " + cache.getNumberOfMessages() + " messages");

      }

      System.out.println("Go check!");
      Thread.sleep(50000);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      System.out.println("Tmp:" + getTemporaryDir());

      server = createServer(true,
                            createDefaultConfig(),
                            PAGE_SIZE,
                            PAGE_MAX,
                            new HashMap<String, AddressSettings>());

      server.start();

      createQueue(ADDRESS.toString(), ADDRESS.toString());
   }

   protected void tearDown() throws Exception
   {
      server.stop();
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
