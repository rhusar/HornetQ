/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.core.server.impl;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.UUID;
import org.hornetq.utils.UUIDGenerator;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 13, 2010
 *         Time: 2:44:02 PM
 */
public class FileLockNodeManager extends NodeManager
{
   private static final Logger log = Logger.getLogger(FileLockNodeManager.class);

   private final String SERVER_LOCK_NAME = "server.lock";

   private static final byte LIVE = 'L';

   private static final byte FAILINGBACK = 'F';

   private static final byte PAUSED = 'P';

   private static final byte NOT_STARTED = 'N';

   private FileChannel channel;

   private FileLock liveLock;

   private FileLock backupLock;

   private final String directory;

   public FileLockNodeManager(final String directory)
   {
      this.directory = directory;
   }

   public void start() throws Exception
   {
      if(isStarted())
      {
         return;
      }
      File file = new File(directory, SERVER_LOCK_NAME);

      if (!file.exists())
      {
         file.createNewFile();
      }

      RandomAccessFile raFile = new RandomAccessFile(file, "rw");

      channel = raFile.getChannel();

      createNodeId();

      super.start();
   }

   public void stop() throws Exception
   {
      channel.close();

      super.stop();
   }


   public void awaitLiveNode() throws Exception
   {
      do
      {
         while (getState() == NOT_STARTED)
         {
            Thread.sleep(2000);
         }

         liveLock = channel.lock(1, 1, false);

         byte state = getState();

         if (state == PAUSED)
         {
            liveLock.release();
            Thread.sleep(2000);
         }
         else if (state == FAILINGBACK)
         {
            liveLock.release();
            Thread.sleep(2000);
         }
         else if (state == LIVE)
         {
            releaseBackupLock();

            break;
         }
      }
      while (true);
   }

   public void startBackup() throws Exception
   {

      log.info("Waiting to become backup node");

      backupLock = channel.lock(2, 1, false);

      log.info("** got backup lock");

      readNodeId();
   }

   public void startLiveNode() throws Exception
   {
      setFailingBack();

      log.info("Waiting to obtain live lock");

      liveLock = channel.lock(1, 1, false);

      log.info("Live Server Obtained live lock");

      setLive();
   }

   public void pauseLiveServer() throws Exception
   {
      setPaused();
      liveLock.release();
   }

   public void crashLiveServer() throws Exception
   {
      //overkill as already set to live
      setLive();
      liveLock.release();
   }

   public void stopBackup() throws Exception
   {
      backupLock.release();
   }

   private void setLive() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(LIVE);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private void setFailingBack() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(FAILINGBACK);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private void setPaused() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(PAUSED);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private byte getState() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      int read;
      read = channel.read(bb, 0);
      if (read <= 0)
      {
         return NOT_STARTED;
      }
      else
         return bb.get(0);
   }

   private void releaseBackupLock() throws Exception
   {
      if (backupLock != null)
      {
         backupLock.release();
      }
   }

   private void createNodeId() throws Exception
   {
      ByteBuffer id = ByteBuffer.allocateDirect(16);
      int read = channel.read(id, 3);
      if(read != 16)
      {
         uuid = UUIDGenerator.getInstance().generateUUID();
         nodeID = new SimpleString(uuid.toString());
         id.put(uuid.asBytes(), 0, 16);
         id.position(0);
         channel.write(id, 3);
         channel.force(true);
      }
      else
      {
         byte[] bytes = new byte[16];
         id.position(0);
         id.get(bytes);
         uuid = new UUID(UUID.TYPE_TIME_BASED, bytes);
         nodeID = new SimpleString(uuid.toString());
      }
   }

   private void readNodeId() throws Exception
   {
      ByteBuffer id = ByteBuffer.allocateDirect(16);
      int read = channel.read(id, 3);
      if(read != 16)
      {
         throw new IllegalStateException("live server did not write id to file");
      }
      else
      {
         byte[] bytes = new byte[16];
         id.position(0);
         id.get(bytes);
         uuid = new UUID(UUID.TYPE_TIME_BASED, bytes);
         nodeID = new SimpleString(uuid.toString());
      }
   }
}

