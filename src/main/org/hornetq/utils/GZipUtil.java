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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Executor;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.logging.Logger;

/**
 * A GZipUtil
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class GZipUtil
{

   private static final Logger log = Logger.getLogger(GZipUtil.class);

   /**
    * This will start a GZipOutputStream, using another thread through a Pipe
    * TODO: We would need an inverted GZipInputStream (that would compress on reading) to avoid creating this thread (through an executor)
    * @param inputStreamParameter
    * @param compress = true if compressing, false if decompressing
    * @return
    * @throws HornetQException
    */
   public static InputStream pipeGZip(final InputStream inputStreamParameter, final boolean compress, final Executor threadPool) throws HornetQException
   {
      final InputStream input;
      if (compress)
      {
         input = inputStreamParameter;
      }
      else
      {
         try
         {
            input = new GZIPInputStream(new BufferedInputStream(inputStreamParameter));
         }
         catch (IOException e)
         {
            throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY, e.getMessage(), e);
         }
      }
      
      final PipedOutputStream pipedOut = new PipedOutputStream();
      final PipedInputStream pipedInput = new PipedInputStream();
      try
      {
         pipedOut.connect(pipedInput);
      }
      catch (IOException e)
      {
         throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY, e.getMessage(), e);
      }
      
      threadPool.execute(new Runnable()
      {
         
         public void run()
         {
            byte readBytes[] = new byte[1024];
            int size = 0;
            
            try
            {
               OutputStream out;
               if (compress)
               {
                  BufferedOutputStream buffOut = new BufferedOutputStream(pipedOut);
                  out = new GZIPOutputStream(buffOut);
               }
               else
               {
                  out = new BufferedOutputStream(pipedOut);
               }
               while ((size = input.read(readBytes)) > 0)
               {
                  System.out.println("Read " + size + " bytes on compressing thread");
                  out.write(readBytes, 0, size);
               }
               System.out.println("Finished compressing");
               out.close();
            }
            catch (Exception e)
            {
               log.warn(e.getMessage());
               try
               {
                  pipedOut.close();
               }
               catch (Exception ignored)
               {
               }
            }
            
         }
      });
      
      return pipedInput;
   }

   public static void deZip(final InputStream input, final OutputStream output, final Executor threadPool) throws HornetQException
   {
      threadPool.execute(new Runnable()
      {
         
         public void run()
         {
            byte readBytes[] = new byte[1024];
            int size = 0;

            OutputStream out = null;
            
            try
            {
               BufferedOutputStream buffOut = new BufferedOutputStream(output);
               out = new GZIPOutputStream(buffOut);
               while ((size = input.read(readBytes)) > 0)
               {
                  System.out.println("Read " + size + " bytes on compressing thread");
                  out.write(readBytes, 0, size);
               }
               System.out.println("Finished compressing");
               out.close();
            }
            catch (Exception e)
            {
               log.warn(e.getMessage());
               try
               {
                  out.close();
               }
               catch (Exception ignored)
               {
               }
            }
            
         }
      });
   }


}
