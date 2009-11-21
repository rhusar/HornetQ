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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;

/**
 * A AbstractSequentialFile
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public abstract class AbstractSequentialFile implements SequentialFile
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(AbstractSequentialFile.class);

   // Attributes ----------------------------------------------------

   private File file;

   private final String directory;
   
   protected final SequentialFileFactory factory;

   protected long fileSize = 0;

   protected final AtomicLong position = new AtomicLong(0);

   protected TimedBuffer timedBuffer;

   /** Instead of having AIOSequentialFile implementing the Observer, I have done it on an inner class.
    *  This is the class returned to the factory when the file is being activated. */
   protected final TimedBufferObserver timedBufferObserver = new LocalBufferObserver();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param file
    * @param directory
    */
   public AbstractSequentialFile(final String directory, final File file, final SequentialFileFactory factory)
   {
      super();
      this.file = file;
      this.directory = directory;
      this.factory = factory;
   }

   // Public --------------------------------------------------------

   public final boolean exists()
   {
      return file.exists();
   }

   public final String getFileName()
   {
      return file.getName();
   }

   public final void delete() throws Exception
   {
      if (isOpen())
      {
         close();
      }

      file.delete();
   }

   public void position(final long pos) throws Exception
   {
      position.set(pos);
   }

   public long position() throws Exception
   {
      return position.get();
   }

   public final void renameTo(final String newFileName) throws Exception
   {
      close();
      File newFile = new File(directory + "/" + newFileName);

      if (!file.equals(newFile))
      {
         file.renameTo(newFile);
         file = newFile;
      }
   }

   public final boolean fits(final int size)
   {
      if (timedBuffer == null)
      {
         return position.get() + size <= fileSize;
      }
      else
      {
         return timedBuffer.checkSize(size);
      }
   }

   public final void disableAutoFlush()
   {
      if (timedBuffer != null)
      {
         timedBuffer.disableAutoFlush();
      }
   }

   public final void enableAutoFlush()
   {
      if (timedBuffer != null)
      {
         timedBuffer.enableAutoFlush();
      }
   }

   public void setTimedBuffer(final TimedBuffer buffer)
   {
      if (timedBuffer != null)
      {
         timedBuffer.setObserver(null);
      }

      timedBuffer = buffer;

      if (buffer != null)
      {
         buffer.setObserver(timedBufferObserver);
      }

   }

   public void write(final HornetQBuffer bytes, final boolean sync, final IOAsyncTask callback) throws Exception
   {
      if (timedBuffer != null)
      {
         timedBuffer.addBytes(bytes.array(), sync, callback);
      }
      else
      {
         ByteBuffer buffer = factory.newBuffer(bytes.capacity());
         buffer.put(bytes.array());
         buffer.rewind();
         writeDirect(buffer, sync, callback);
      }
   }

   public void write(final HornetQBuffer bytes, final boolean sync) throws Exception
   {
      if (sync)
      {
         SimpleWaitIOCallback completion = new SimpleWaitIOCallback();

         write(bytes, true, completion);

         completion.waitCompletion();
      }
      else
      {
         write(bytes, false, DummyCallback.getInstance());
      }
   }
   
   /**
    * invoke the callback after all pending operations are complete.
    */
   public void syncCallback(IOAsyncTask callback)
   {
      if (timedBuffer != null)
      {
         timedBuffer.syncCallback(callback);
      }
      else
      {
         syncCallbackDirect(callback);
      }
   }
   


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract void syncCallbackDirect(IOAsyncTask callback);

   protected File getFile()
   {
      return file;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected static class DelegateCallback implements IOAsyncTask
   {
      final List<IOAsyncTask> delegates;

      DelegateCallback(final List<IOAsyncTask> delegates)
      {
         this.delegates = delegates;
      }

      public void done()
      {
         for (IOAsyncTask callback : delegates)
         {
            try
            {
               callback.done();
            }
            catch (Throwable e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      }

      public void onError(final int errorCode, final String errorMessage)
      {
         for (IOAsyncTask callback : delegates)
         {
            try
            {
               callback.onError(errorCode, errorMessage);
            }
            catch (Throwable e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      }

      public void waitCompletion() throws Exception
      {
      }
   }

   protected class LocalBufferObserver implements TimedBufferObserver
   {
      public void flushBuffer(final ByteBuffer buffer, final boolean requestedSync, final List<IOAsyncTask> callbacks)
      {
         if (buffer == null)
         {
            syncCallbackDirect(new DelegateCallback(callbacks));
         }
         else
         {
            buffer.flip();
   
            if (buffer.limit() == 0)
            {
               factory.releaseBuffer(buffer);
            }
            else
            {
               writeDirect(buffer, requestedSync, new DelegateCallback(callbacks));
            }
         }
      }

      public ByteBuffer newBuffer(int size, int limit)
      {
         size = factory.calculateBlockSize(size);
         limit = factory.calculateBlockSize(limit);

         ByteBuffer buffer = factory.newBuffer(size);
         buffer.limit(limit);
         return buffer;
      }

      public int getRemainingBytes()
      {
         if (fileSize - position.get() > Integer.MAX_VALUE)
         {
            return Integer.MAX_VALUE;
         }
         else
         {
            return (int)(fileSize - position.get());
         }
      }

      @Override
      public String toString()
      {
         return "TimedBufferObserver on file (" + getFile().getName() + ")";
      }

   }

}
