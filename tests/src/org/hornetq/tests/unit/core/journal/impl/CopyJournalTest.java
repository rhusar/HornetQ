/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.tests.unit.core.journal.impl;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.utils.VariableLatch;

/**
 * A CopyJournalTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class CopyJournalTest extends JournalImplTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   final AtomicInteger sequence = new AtomicInteger(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSimpleCopy() throws Exception
   {
      setup(2, 100 * 1024, false);
      createJournal();
      startJournal();
      load();

      ArrayList<Long> transactions = new ArrayList<Long>();

      ArrayList<Long> toDelete = new ArrayList<Long>();

      for (int i = 0; i < 40; i++)
      {
         long iDelete = sequence.incrementAndGet();

         toDelete.add(iDelete);

         addWithSize(1024, iDelete);
      }

      long tx = sequence.incrementAndGet();
      transactions.add(tx);
      addTx(tx, sequence.incrementAndGet(), sequence.incrementAndGet(), sequence.incrementAndGet());

      for (int i = 0; i < 10; i++)
      {
         addWithSize(1024,
                     sequence.incrementAndGet(),
                     sequence.incrementAndGet(),
                     sequence.incrementAndGet(),
                     sequence.incrementAndGet());
      }

      File destDir = new File(getTestDir() + "/dest");

      destDir.mkdirs();

      SequentialFileFactory newFactory = new NIOSequentialFileFactory(destDir.getAbsolutePath());

      Journal destJournal = new JournalImpl(10 * 1024, 2, 0, 0, newFactory, filePrefix, fileExtension, 1);
      destJournal.start();
      destJournal.loadInternalOnly();

      CountDownLatch locked = new CountDownLatch(1);

      JournalHandler handler = new JournalHandler(destJournal, locked, 5);

      final Journal proxyJournal = (Journal)Proxy.newProxyInstance(this.getClass().getClassLoader(),
                                                                   new Class[] { Journal.class },
                                                                   handler);
      
      final AtomicBoolean copied = new AtomicBoolean(false);

      Thread copier = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               journal.copyTo(proxyJournal, new Runnable()
               {
                  public void run()
                  {
                     copied.set(true);
                  }
               });
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }

         }
      };

      copier.start();

      assertTrue(locked.await(10, TimeUnit.SECONDS));

      sequence.set(5000);

      for (int i = 0; i < 10; i++)
      {

         tx = sequence.incrementAndGet();
         transactions.add(tx);
         addTx(tx, sequence.incrementAndGet(), sequence.incrementAndGet(), sequence.incrementAndGet());

         journal.forceMoveNextFile();
      }

      for (Long txToCommit : transactions)
      {
         commit(txToCommit);
      }

      for (Long iDelete : toDelete)
      {
         delete(iDelete);
      }

      handler.unlock();

      copier.join();
      
      assertTrue(copied.get());

      stopJournal();

      destJournal.stop();

      fileFactory = newFactory;

      createJournal();

      startJournal();

      loadAndCheck(true);
   }

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();

      return new AIOSequentialFileFactory(getTestDir(),
                                          ConfigurationImpl.DEFAULT_JOURNAL_AIO_BUFFER_SIZE,
                                          1000000,
                                          false,
                                          false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /** This handler will lock after N calls, until the Handler is opened again */
   protected class JournalHandler implements InvocationHandler
   {

      final VariableLatch valve = new VariableLatch();

      final CountDownLatch locked;

      final int executionsBeforeLock;

      int executions = 0;

      private final Journal target;

      public JournalHandler(final Journal journal, final CountDownLatch locked, final int executionsBeforeLock)
      {
         target = journal;
         this.locked = locked;
         this.executionsBeforeLock = executionsBeforeLock;
      }

      private void lock()
      {
         valve.up();
      }

      public void unlock()
      {
         valve.down();
      }

      public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
      {
         if (executions++ == executionsBeforeLock)
         {
            lock();
            locked.countDown();
         }
         if (!valve.waitCompletion(10000))
         {
            throw new IllegalStateException("Timeout waiting for open valve");
         }
         return method.invoke(target, args);
      }

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
