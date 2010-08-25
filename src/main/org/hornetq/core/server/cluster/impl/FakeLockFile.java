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
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.core.server.cluster.LockFile;

/**
 * A FakeLockFile
 * 
 * A VM-wide exclusive lock on a file.
 * 
 * Advisory only.
 * 
 * Used for testing.
 *
 * @author Tim Fox
 *
 *
 */
public class FakeLockFile implements LockFile
{
   private final String fileName;

   private final String directory;
   
   private final static Map<String, Semaphore> locks = new WeakHashMap<String, Semaphore>();

   private Semaphore semaphore;
   /**
    * @param fileName
    * @param directory
    */
   public FakeLockFile(final String fileName, final String directory)
   {
      this.fileName = fileName;
      
      this.directory = directory;
      
      synchronized (locks)
      {
         String key = directory + "/" + fileName;
         
         semaphore = locks.get(key);
         
         if (semaphore == null)
         {
            semaphore = new Semaphore(1, true);
            
            locks.put(key, semaphore);

            File f = new File(directory, fileName);

            try
            {
               f.createNewFile();
            }
            catch (IOException e)
            {
               e.printStackTrace();
               throw new IllegalStateException(e);
            }

            if(!f.exists())
            {
               throw new IllegalStateException("unable to create " + directory + fileName);
            }
         }
      }
   }
   
   public String getFileName()
   {
      return fileName;
   }

   public String getDirectory()
   {
      return directory;
   }

   public void lock() throws IOException
   {
      try
      {
         semaphore.acquire();
      }
      catch (InterruptedException e)
      {
         throw new IOException(e);
      }
   }

   public boolean unlock() throws IOException
   {
      semaphore.release();
      
      return true;
   }

   public static void unlock(final String fileName, final String directory)
   {
      String key = directory + "/" + fileName;

      Semaphore semaphore = locks.get(key);

      semaphore.release();
   }

   public static void clearLocks()
   {
      for (Semaphore semaphore : locks.values())
      {
         semaphore.drainPermits();
      }
      locks.clear();
   }
}
