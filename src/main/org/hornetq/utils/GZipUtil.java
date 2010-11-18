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

package org.hornetq.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.hornetq.api.core.HornetQException;

/**
 * A GZipUtil
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class GZipUtil
{
   public static InputStream createZipInputStream(InputStream input) throws HornetQException
   {
      try
      {
         return new GZipPipe(input, 1024);
      }
      catch (IOException e)
      {
         throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY, e.getMessage(), e);
      }
   }
   
   public static InputStream createUnZipInputStream(InputStream input) throws HornetQException
   {
      try
      {
         return new GZIPInputStream(input);
      }
      catch (IOException e)
      {
         throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY, e.getMessage(), e);
      }
   }
   
   public static OutputStream createZipOutputStream(OutputStream out) throws HornetQException
   {
      try
      {
         return new GZipOutput(out);
      }
      catch (IOException e)
      {
         throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY, e.getMessage(), e);
      }      
   }

   public static class GZipOutput extends OutputStream
   {
      private OutputStream target;
      private GZIPInputStream zipIn;
      private DynamicInputStream receiver;

      public GZipOutput(OutputStream out) throws IOException
      {
         target = out;
         receiver = new DynamicInputStream(1024, 50);
      }

      public void write(int b) throws IOException
      {
         receiver.writeBuffer(b);
      }
      
      public void close() throws IOException
      {
         zipIn = new GZIPInputStream(receiver);
         byte[] buffer = new byte[1024];
         int n = zipIn.read(buffer);
         while (n != -1)
         {
            if (n > 0)
            {
               target.write(buffer, 0, n);
            }
            n = zipIn.read(buffer);
         }
         target.close();
      }
      
   }
   
   public static class DynamicInputStream extends InputStream
   {
      private List<byte[]> writeBuffer;
      private int bufferSize;
      private int counter, index;
      private int readIndex, readCounter;
      
      public DynamicInputStream(int size, int cache)
      {
         bufferSize = size;
         writeBuffer = new ArrayList<byte[]>(cache);
         for (int i = 0; i < cache; i++)
         {
            writeBuffer.add(new byte[size]);
         }
         counter = 0;
         index = 0;
         readIndex = 0;
         readCounter = 0;
      }

      //read the buffer. If buffer is empty, return -1
      public int read() throws IOException
      {
         int result = -1;
         
         if (index > readIndex)
         {
            result = writeBuffer.get(readIndex)[readCounter++] & 0xFF;
            if (readCounter == bufferSize)
            {
               readCounter = 0;
               readIndex ++;
            }
         }
         else if (index == readIndex)
         {
            if (counter > readCounter)
            {
               result = writeBuffer.get(readIndex)[readCounter++] & 0xFF;
            }
         }
         return result;
      }
      
      public void writeBuffer(int b)
      {
         writeBuffer.get(index)[counter++] = (byte)b;
         if (counter == bufferSize)
         {
            index++;
            if (index == writeBuffer.size())
            {
               writeBuffer.add(new byte[bufferSize]);
            }
            counter = 0;
         }
      }
      
   }
   
   /*
    * we keep a list of byte arrays. when writing, we start with the first array.
    * when getBuffer() is called, the returned value is subject to the following rules:
    * 
    * 1. if not closed, return the last full array. then update flags and pointers.
    * 2. if closed, return all the remaining.
    */
   public static class DynamicOutputStream extends OutputStream
   {

      private List<byte[]> writeBuffer;
      private int bufferSize;
      private int counter, index;
      private int readIndex;
      private boolean closed;
      
      public DynamicOutputStream(int size, int cache)
      {
         bufferSize = size;
         writeBuffer = new ArrayList<byte[]>(cache);
         for (int i = 0; i < cache; i++)
         {
            writeBuffer.add(new byte[size]);
         }
         counter = 0;
         index = 0;
         readIndex = 0;
         closed = false;
      }

      public void write(int b) throws IOException
      {
         writeBuffer.get(index)[counter++] = (byte)b;
         if (counter == bufferSize)
         {
            index++;
            if (index == writeBuffer.size())
            {
               writeBuffer.add(new byte[bufferSize]);
            }
            counter = 0;
         }
      }
      
      public void close() throws IOException
      {
         closed = true;
      }

      /*
       * logic: 
       * if index > readIndex, return readIndex, then readIndex++
       * if index == readIndex, then return zero length byte[]; if closed, return the remaining.
       * 
       * if closed and no more data, returns null.
       */
      public byte[] getBuffer()
      {
         byte[] result = new byte[0];
         if (index > readIndex)
         {
            result = writeBuffer.get(readIndex);
            writeBuffer.set(readIndex, null);
            readIndex++;
         }
         else if (index == readIndex)
         {
            if (closed)
            {
               if (counter == 0)
               {
                  result = null;
               }
               else
               {
                  result = new byte[counter];
                  System.arraycopy(writeBuffer.get(index), 0, result, 0, result.length);
                  counter = 0;
               }
            }
         }
         return result;
      }
   }
   
   public static class GZipPipe extends InputStream
   {
      private InputStream input;
      private byte[] readBuffer;
      private GZIPOutputStream zipOut;
      private DynamicOutputStream receiver;
      private int readPointer;
      private byte[] buffer;
      
      public GZipPipe(InputStream raw, int size) throws IOException
      {
         input = raw;
         readBuffer = new byte[size];
         receiver = new DynamicOutputStream(size, 50);
         zipOut = new GZIPOutputStream(receiver);
         readPointer = 0;
         buffer = read1();
      }
      
      public int read() throws IOException
      {
         if (buffer == null)
         {
            return -1;
         }
         
         int val = buffer[readPointer] & 0xFF;
         readPointer++;
         if (readPointer == buffer.length)
         {
            buffer = read1();
            readPointer = 0;
         }

         return val;
      }
      
      public byte[] read1() throws IOException
      {
         byte[] result = receiver.getBuffer();
         if (result == null)
         {
            return null;
         }
         else if (result.length > 0)
         {
            return result;
         }
         
         int n = input.read(readBuffer);
         while (true)
         {
            if (n > 0)
            {
               zipOut.write(readBuffer, 0, n);
               result = receiver.getBuffer();
               if ((result != null) && (result.length > 0))
               {
                  break;
               }
               n = input.read(readBuffer);
            }
            else
            {
               zipOut.close();
               result = receiver.getBuffer();
               break;
            }
         }
         return result;
      }
   }

   public static void main(String[] args) throws HornetQException, IOException
   {
      long begin = System.currentTimeMillis();
/*
      FileInputStream input = new FileInputStream("/home/howard/tmp/jbm.log.1");
      FileOutputStream output = new FileOutputStream("/home/howard/tmp/output3.zip");
      GZIPOutputStream zipOut = new GZIPOutputStream(output);
      
      byte[] buffer = new byte[1024];
      
      int n = input.read(buffer);
      
      int counter = 0;
      
      while (n > 0)
      {
         zipOut.write(buffer, 0, n);
         counter += n;
         n = input.read(buffer);
      }
      zipOut.close();
      
      System.out.println("----total output: " + counter);
*/
      zip();
/*
      FileInputStream input = new FileInputStream("/home/howard/tmp/jbm.log.1");
      FileOutputStream output = new FileOutputStream("/home/howard/tmp/output.zip");
      ExecutorService service = Executors.newCachedThreadPool();
      InputStream result = GZipUtil.pipeGZip(input, true, service);
      
      byte[] buffer = new byte[2048];
      int n = result.read(buffer);
      System.out.println("got first data");
      
      while (n > 0)
      {
         output.write(buffer);
         n = result.read(buffer);
      }
*/
      long end = System.currentTimeMillis();
      
      
      System.out.println("done. time: " + (end - begin));
   }
   
   public static void zip() throws IOException
   {
      FileInputStream input = new FileInputStream("/home/howard/tmp/jbm.log.1");
      FileOutputStream output = new FileOutputStream("/home/howard/tmp/myzip.zip");
      GZipPipe pipe = new GZipPipe(input, 2048);
      
      byte[] buffer = new byte[2048];
      
      int n = pipe.read(buffer);
      
      while (n != -1)
      {
         if (n > 0)
         {
            output.write(buffer, 0, n);
         }
         n = pipe.read(buffer);
      }

      output.close();
   }

   public static void unzip() throws IOException
   {
      FileInputStream input = new FileInputStream("/home/howard/tmp/myzip.zip");
      FileOutputStream output = new FileOutputStream("/home/howard/tmp/myzip.out");

      GZIPInputStream zipIn = new GZIPInputStream(input);
      
      byte[] buffer = new byte[1024];
      
      int n = zipIn.read(buffer);
      
      while (n > 0)
      {
         //System.out.println("buffer size: " + buffer.length);
         output.write(buffer, 0, n);
         n = zipIn.read(buffer);
      }

      output.close();
   }

}
