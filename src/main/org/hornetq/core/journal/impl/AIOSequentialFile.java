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
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.asyncio.AsynchronousFile;
import org.hornetq.core.asyncio.BufferCallback;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;

/**
 * 
 * A AIOSequentialFile
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFile extends AbstractSequentialFile
{
   private static final Logger log = Logger.getLogger(AIOSequentialFile.class);

   private boolean opened = false;

   private final int maxIO;

   private AsynchronousFile aioFile;

   private final BufferCallback bufferCallback;

   /** The pool for Thread pollers */
   private final Executor pollerExecutor;
   
   /** Context switch on AIO could fire unnecessary flushes, so we use a single thread for write */
   private final Executor writerExecutor;
   

   public AIOSequentialFile(final SequentialFileFactory factory,
                            final int bufferSize,
                            final long bufferTimeoutMilliseconds,
                            final String directory,
                            final String fileName,
                            final int maxIO,
                            final BufferCallback bufferCallback,
                            final Executor callbackExecutor,
                            final Executor writerExecutor,
                            final Executor pollerExecutor)
   {
      super(callbackExecutor, directory, new File(directory + "/" + fileName), factory);
      this.maxIO = maxIO;
      this.writerExecutor = writerExecutor;
      this.bufferCallback = bufferCallback;
      this.pollerExecutor = pollerExecutor;
   }

   public boolean isOpen()
   {
      return opened;
   }

   public int getAlignment() throws Exception
   {
      checkOpened();

      return aioFile.getBlockSize();
   }

   public int calculateBlockStart(final int position) throws Exception
   {
      int alignment = getAlignment();

      int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

      return pos;
   }

   public SequentialFile copy()
   {
      return new AIOSequentialFile(factory, -1, -1, getFile().getParent(), getFileName(), maxIO, bufferCallback, callbackExecutor, writerExecutor, pollerExecutor);
   }

   public synchronized void close() throws Exception
   {
      if (!opened)
      {
         return;
      }
      opened = false;

      timedBuffer = null;

      final CountDownLatch donelatch = new CountDownLatch(1);

      writerExecutor.execute(new Runnable()
      {
         public void run()
         {
            donelatch.countDown();
         }
      });

      while (!donelatch.await(60, TimeUnit.SECONDS))
      {
         log.warn("Executor on file " + getFile().getName() + " couldn't complete its tasks in 60 seconds.",
                  new Exception("Warning: Executor on file " + getFile().getName() +
                                " couldn't complete its tasks in 60 seconds."));
      }

      aioFile.close();
      aioFile = null;

      this.notifyAll();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFile#waitForClose()
    */
   public synchronized void waitForClose() throws Exception
   {
      while (isOpen())
      {
         wait();
      }
   }

   public void fill(final int position, final int size, final byte fillCharacter) throws Exception
   {
      checkOpened();

      int fileblockSize = aioFile.getBlockSize();

      int blockSize = fileblockSize;

      if (size % (100 * 1024 * 1024) == 0)
      {
         blockSize = 100 * 1024 * 1024;
      }
      else if (size % (10 * 1024 * 1024) == 0)
      {
         blockSize = 10 * 1024 * 1024;
      }
      else if (size % (1024 * 1024) == 0)
      {
         blockSize = 1024 * 1024;
      }
      else if (size % (10 * 1024) == 0)
      {
         blockSize = 10 * 1024;
      }
      else
      {
         blockSize = fileblockSize;
      }

      int blocks = size / blockSize;

      if (size % blockSize != 0)
      {
         blocks++;
      }

      int filePosition = position;

      if (position % fileblockSize != 0)
      {
         filePosition = (position / fileblockSize + 1) * fileblockSize;
      }

      aioFile.fill(filePosition, blocks, blockSize, fillCharacter);

      this.fileSize = aioFile.size();
   }

   public void open() throws Exception
   {
      open(maxIO);
   }

   public synchronized void open(final int currentMaxIO) throws Exception
   {
      opened = true;
      aioFile = new AsynchronousFileImpl(writerExecutor, pollerExecutor, callbackExecutor);
      aioFile.open(getFile().getAbsolutePath(), currentMaxIO);
      position.set(0);
      aioFile.setBufferCallback(bufferCallback);
      this.fileSize = aioFile.size();
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      aioFile.setBufferCallback(callback);
   }

   public int read(final ByteBuffer bytes, final IOAsyncTask callback) throws Exception
   {
      int bytesToRead = bytes.limit();

      long positionToRead = position.getAndAdd(bytesToRead);

      bytes.rewind();

      aioFile.read(positionToRead, bytesToRead, bytes, callback);

      return bytesToRead;
   }

   public int read(final ByteBuffer bytes) throws Exception
   {
      SimpleWaitIOCallback waitCompletion = new SimpleWaitIOCallback();

      int bytesRead = read(bytes, waitCompletion);

      waitCompletion.waitCompletion();

      return bytesRead;
   }
   

   public void sync() throws Exception
   {
      throw new IllegalArgumentException("This method is not supported on AIO");
   }

   public long size() throws Exception
   {
      if (aioFile == null)
      {
         return getFile().length();
      }
      else
      {
         return aioFile.size();
      }
   }

   @Override
   public String toString()
   {
      return "AIOSequentialFile:" + getFile().getAbsolutePath();
   }

   // Public methods
   // -----------------------------------------------------------------------------------------------------

   // Protected methods
   // -----------------------------------------------------------------------------------------------------

   
   public void writeDirect(final ByteBuffer bytes, final boolean sync) throws Exception
   {
      if (sync)
      {
         SimpleWaitIOCallback completion = new SimpleWaitIOCallback();
  
         writeDirect(bytes, true, completion);
  
         completion.waitCompletion();
      }
      else
      {
         writeDirect(bytes, false, DummyCallback.getInstance());
      }
   }

   
   /**
    * 
    * @param sync Not used on AIO
    *  */
   public void writeDirect(final ByteBuffer bytes, final boolean sync, IOAsyncTask callback)
   {
      final int bytesToWrite = factory.calculateBlockSize(bytes.limit());

      final long positionToWrite = position.getAndAdd(bytesToWrite);

      aioFile.write(positionToWrite, bytesToWrite, bytes, callback);
   }


   // Private methods
   // -----------------------------------------------------------------------------------------------------

   private void checkOpened() throws Exception
   {
      if (aioFile == null || !opened)
      {
         throw new IllegalStateException("File not opened");
      }
   }
}
