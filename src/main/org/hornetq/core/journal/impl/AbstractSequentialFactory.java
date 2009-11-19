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

package org.hornetq.core.journal.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.HornetQThreadFactory;

/**
 * 
 * An abstract SequentialFileFactory containing basic functionality for both AIO and NIO SequentialFactories
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public abstract class AbstractSequentialFactory implements SequentialFileFactory
{

   // Timeout used to wait executors to shutdown
   protected static final int EXECUTOR_TIMEOUT = 60;

   private static final Logger log = Logger.getLogger(AbstractSequentialFactory.class);

   /** For AIO: A single AIO write executor for every AIO File.
    *  This is used only for AIO & instant operations. We only need one executor-thread for the entire journal as we always have only one active file.
    *  And even if we had multiple files at a given moment, this should still be ok, as we control max-io in a semaphore, guaranteeing AIO calls don't block on disk calls.
    *  
    *  For NIO: this is used to execute the callbacks.
    *           We can't call the executor holding a lock.
    *   */
   protected ExecutorService writeExecutor;

   protected final String journalDir;

   protected final TimedBuffer timedBuffer;

   protected final int bufferSize;

   protected final long bufferTimeout;

   public AbstractSequentialFactory(final String journalDir,
                                    final boolean buffered,
                                    final int bufferSize,
                                    final long bufferTimeout,
                                    final boolean flushOnSync,
                                    final boolean logRates)
   {
      this.journalDir = journalDir;
      if (buffered)
      {
         timedBuffer = new TimedBuffer(bufferSize, bufferTimeout, flushOnSync, logRates);
      }
      else
      {
         timedBuffer = null;
      }
      this.bufferSize = bufferSize;
      this.bufferTimeout = bufferTimeout;
   }

   public void stop()
   {
      if (timedBuffer != null)
      {
         timedBuffer.stop();
      }

      if (writeExecutor != null)
      {
         writeExecutor.shutdown();

         try
         {
            if (!writeExecutor.awaitTermination(EXECUTOR_TIMEOUT, TimeUnit.SECONDS))
            {
               log.warn("Timed out on AIO writer shutdown", new Exception("Timed out on AIO writer shutdown"));
            }
         }
         catch (InterruptedException e)
         {
         }
      }
   }

   public void start()
   {
      if (timedBuffer != null)
      {
         timedBuffer.start();
      }

      if (isSupportsCallbacks())
      {
         writeExecutor = Executors.newSingleThreadExecutor(new HornetQThreadFactory("HornetQ-writer-pool" + System.identityHashCode(this),
                                                                                    true));
      }
      else
      {
         writeExecutor = null;
      }

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFileFactory#activate(org.hornetq.core.journal.SequentialFile)
    */
   public void activateBuffer(final SequentialFile file)
   {
      if (timedBuffer != null)
      {
         timedBuffer.disableAutoFlush();
         try
         {
            file.setTimedBuffer(timedBuffer);
         }
         finally
         {
            file.enableAutoFlush();
         }
      }
   }

   public void flush()
   {
      if (timedBuffer != null)
      {
         timedBuffer.flush();
      }
   }

   public void deactivateBuffer()
   {
      if (timedBuffer != null)
      {
         timedBuffer.flush();
         timedBuffer.setObserver(null);
      }
   }

   public void releaseBuffer(ByteBuffer buffer)
   {
   }

   /** 
    * Create the directory if it doesn't exist yet
    */
   public void createDirs() throws Exception
   {
      File file = new File(journalDir);
      boolean ok = file.mkdirs();
      if (!ok)
      {
         throw new IOException("Failed to create directory " + journalDir);
      }
   }

   public List<String> listFiles(final String extension) throws Exception
   {
      File dir = new File(journalDir);

      FilenameFilter fnf = new FilenameFilter()
      {
         public boolean accept(final File file, final String name)
         {
            return name.endsWith("." + extension);
         }
      };

      String[] fileNames = dir.list(fnf);

      if (fileNames == null)
      {
         throw new IOException("Failed to list: " + journalDir);
      }

      return Arrays.asList(fileNames);
   }

}
