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

package org.hornetq.core.server.cluster.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.cluster.LockFile;

/**
 * A FailoverLockFileImpl
 * 
 * The lock is per VM!
 * 
 * Won't work well with NFS or GFS
 *
 * @author Tim Fox
 *
 */
public class LockFileImpl implements LockFile
{
   private static final Logger log = Logger.getLogger(LockFileImpl.class);

   private final String fileName;

   private final String directory;

   private RandomAccessFile raFile;

   private FileLock lock;

   /*
    * This method is "mainly" for testing (apologies for pun)
    */
   public static final void main(String[] args)
   {
      LockFileImpl lock = new LockFileImpl(args[0], args[1]);

      long time = Long.parseLong(args[2]);

      try
      {
         lock.lock();
      }
      catch (IOException e)
      {
         log.error("Failed to get lock", e);
      }

      log.info("Sleeping for " + time + " ms");

      try
      {
         Thread.sleep(time);
      }
      catch (InterruptedException e)
      {
      }

      try
      {
         lock.unlock();
      }
      catch (IOException e)
      {
         log.error("Failed to unlock", e);
      }
   }

   /**
    * @param fileName
    * @param directory
    */
   public LockFileImpl(final String fileName, final String directory)
   {
      this.fileName = fileName;

      this.directory = directory;
   }

   public String getFileName()
   {
      return fileName;
   }

   public String getDirectory()
   {
      return directory;
   }

   private final Object lockLock = new Object();

   private final Object unlockLock = new Object();

   public void lock() throws IOException
   {
      synchronized (lockLock)
      {
         File file = new File(directory, fileName);

         log.info("Trying to create " + file.getCanonicalPath());

         if (!file.exists())
         {
            file.createNewFile();
         }

         raFile = new RandomAccessFile(file, "rw");

         FileChannel channel = raFile.getChannel();

         // Try and obtain exclusive lock
         log.info("Trying to obtain exclusive lock on " + fileName);

         lock = channel.lock();

         log.info("obtained lock");
      }
   }

   public boolean unlock() throws IOException
   {
      synchronized (unlockLock)
      {
         if (lock == null)
         {
            return false;
         }

         lock.release();

         lock = null;

         raFile.close();

         raFile = null;

         log.info("Released lock on " + fileName);

         return true;
      }
   }

}
