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
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;

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
      setup(10, 10 * 1024, true);
      createJournal();
      startJournal();
      load();
      
      

      for (int i = 0 ; i < 10; i++)
      {
         addWithSize(1024, sequence.incrementAndGet(), sequence.incrementAndGet(), sequence.incrementAndGet(), sequence.incrementAndGet());
      }
      addTx(sequence.incrementAndGet(), sequence.incrementAndGet(), sequence.incrementAndGet(), sequence.incrementAndGet());
      
      File destDir = new File(getTestDir()+"/dest");
      
      destDir.mkdirs();
      
      SequentialFileFactory nioFactory = new NIOSequentialFileFactory(destDir.getAbsolutePath());
      
      Journal destJournal = new JournalImpl(10 * 1024, 2, 0, 0, nioFactory, filePrefix, fileExtension, 1);
      destJournal.start();
      destJournal.loadInternalOnly();
      
      journal.copyTo(destJournal);
      
      journal.flush();
      
      destJournal.flush();
      
      
      
      System.exit(1);
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
                                          true,
                                          false      
      );
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
