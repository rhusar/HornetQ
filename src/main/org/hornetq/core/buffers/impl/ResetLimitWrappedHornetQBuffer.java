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

package org.hornetq.core.buffers.impl;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.logging.Logger;

/**
 * A ResetLimitWrappedHornetQBuffer
 *
 * @author Tim Fox
 *
 */
public class ResetLimitWrappedHornetQBuffer extends ChannelBufferWrapper
{      
   private static final Logger log = Logger.getLogger(ResetLimitWrappedHornetQBuffer.class);

   private final int limit;
      
   public ResetLimitWrappedHornetQBuffer(final int limit, final HornetQBuffer buffer)
   {
      super(buffer.channelBuffer());
      
      this.limit = limit;
      
      if (writerIndex() < limit)
      {
         writerIndex(limit);
      }
      
      readerIndex(limit);
   }
   
   public void setBuffer(HornetQBuffer buffer)
   {      
      this.buffer = buffer.channelBuffer();
   }
   
   public void clear()
   {
      buffer.clear();
      
      buffer.setIndex(limit, limit);
   }

   public void readerIndex(int readerIndex)
   {
      if (readerIndex < limit)
      {
         readerIndex = limit;
      }
      
      buffer.readerIndex(readerIndex);
   }

   public void resetReaderIndex()
   {
      buffer.readerIndex(limit);
   }

   public void resetWriterIndex()
   {
      buffer.writerIndex(limit);
   }

   public void setIndex(int readerIndex, int writerIndex)
   {
      if (readerIndex < limit)
      {
         readerIndex = limit;
      }
      if (writerIndex < limit)
      {
         writerIndex = limit;
      }
      buffer.setIndex(readerIndex, writerIndex);
   }

   public void writerIndex(int writerIndex)
   {
      if (writerIndex < limit)
      {
         writerIndex = limit;
      }
      buffer.writerIndex(writerIndex);
   }
}
