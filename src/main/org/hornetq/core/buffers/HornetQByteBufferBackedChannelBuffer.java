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

package org.hornetq.core.buffers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.UnsupportedCharsetException;

import org.hornetq.core.remoting.spi.HornetQBuffer;

/**
 * A NIO {@link ByteBuffer} based buffer.  It is recommended to use {@link HornetQChannelBuffers#directBuffer(int)}
 * and {@link HornetQChannelBuffers#wrappedBuffer(ByteBuffer)} instead of calling the
 * constructor explicitly.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 486 $, $Date: 2008-11-16 22:52:47 +0900 (Sun, 16 Nov 2008) $
 *
 */
public class HornetQByteBufferBackedChannelBuffer extends HornetQAbstractChannelBuffer
{

   private final ByteBuffer buffer;

   private final int capacity;

   /**
    * Creates a new buffer which wraps the specified buffer's slice.
    */
   HornetQByteBufferBackedChannelBuffer(final ByteBuffer buffer)
   {
      if (buffer == null)
      {
         throw new NullPointerException("buffer");
      }

      this.buffer = buffer;
      capacity = buffer.remaining();
   }

   public int capacity()
   {
      return capacity;
   }

   public byte getByte(final int index)
   {
      return buffer.get(index);
   }

   public short getShort(final int index)
   {
      return buffer.getShort(index);
   }

   public int getUnsignedMedium(final int index)
   {
      return (getByte(index) & 0xff) << 16 | (getByte(index + 1) & 0xff) << 8 | (getByte(index + 2) & 0xff) << 0;
   }

   public int getInt(final int index)
   {
      return buffer.getInt(index);
   }

   public long getLong(final int index)
   {
      return buffer.getLong(index);
   }
   
   public void getBytes(final int index, final HornetQChannelBuffer dst, final int dstIndex, final int length)
   {
      if (dst instanceof HornetQByteBufferBackedChannelBuffer)
      {
         HornetQByteBufferBackedChannelBuffer bbdst = (HornetQByteBufferBackedChannelBuffer)dst;
         ByteBuffer data = bbdst.buffer.duplicate();

         data.limit(dstIndex + length).position(dstIndex);
         getBytes(index, data);
      }
      else if (buffer.hasArray())
      {
         dst.setBytes(dstIndex, buffer.array(), index + buffer.arrayOffset(), length);
      }
      else
      {
         dst.setBytes(dstIndex, this, index, length);
      }
   }

   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      ByteBuffer data = buffer.duplicate();
      try
      {
         data.limit(index + length).position(index);
      }
      catch (IllegalArgumentException e)
      {
         throw new IndexOutOfBoundsException();
      }
      data.get(dst, dstIndex, length);
   }

   public void getBytes(final int index, final ByteBuffer dst)
   {
      ByteBuffer data = buffer.duplicate();
      int bytesToCopy = Math.min(capacity() - index, dst.remaining());
      try
      {
         data.limit(index + bytesToCopy).position(index);
      }
      catch (IllegalArgumentException e)
      {
         throw new IndexOutOfBoundsException();
      }
      dst.put(data);
   }

   public void setByte(final int index, final byte value)
   {
      buffer.put(index, value);
   }

   public void setShort(final int index, final short value)
   {
      buffer.putShort(index, value);
   }

   public void setMedium(final int index, final int value)
   {
      setByte(index, (byte)(value >>> 16));
      setByte(index + 1, (byte)(value >>> 8));
      setByte(index + 2, (byte)(value >>> 0));
   }

   public void setInt(final int index, final int value)
   {
      buffer.putInt(index, value);
   }

   public void setLong(final int index, final long value)
   {
      buffer.putLong(index, value);
   }

   public void setBytes(final int index, final HornetQChannelBuffer src, final int srcIndex, final int length)
   {
      if (src instanceof HornetQByteBufferBackedChannelBuffer)
      {
         HornetQByteBufferBackedChannelBuffer bbsrc = (HornetQByteBufferBackedChannelBuffer)src;
         ByteBuffer data = bbsrc.buffer.duplicate();

         data.limit(srcIndex + length).position(srcIndex);
         setBytes(index, data);
      }
      else if (buffer.hasArray())
      {
         src.getBytes(srcIndex, buffer.array(), index + buffer.arrayOffset(), length);
      }
      else
      {
         src.getBytes(srcIndex, this, index, length);
      }
   }

   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      ByteBuffer data = buffer.duplicate();
      data.limit(index + length).position(index);
      data.put(src, srcIndex, length);
   }

   public void setBytes(final int index, final ByteBuffer src)
   {
      ByteBuffer data = buffer.duplicate();
      data.limit(index + src.remaining()).position(index);
      data.put(src);
   }

   public void getBytes(final int index, final OutputStream out, final int length) throws IOException
   {
      if (length == 0)
      {
         return;
      }

      if (!buffer.isReadOnly() && buffer.hasArray())
      {
         out.write(buffer.array(), index + buffer.arrayOffset(), length);
      }
      else
      {
         byte[] tmp = new byte[length];
         ((ByteBuffer)buffer.duplicate().position(index)).get(tmp);
         out.write(tmp);
      }
   }

   public int getBytes(final int index, final GatheringByteChannel out, final int length) throws IOException
   {
      if (length == 0)
      {
         return 0;
      }

      return out.write((ByteBuffer)buffer.duplicate().position(index).limit(index + length));
   }

   public int setBytes(int index, final InputStream in, int length) throws IOException
   {

      int readBytes = 0;

      if (!buffer.isReadOnly() && buffer.hasArray())
      {
         index += buffer.arrayOffset();
         do
         {
            int localReadBytes = in.read(buffer.array(), index, length);
            if (localReadBytes < 0)
            {
               if (readBytes == 0)
               {
                  return -1;
               }
               else
               {
                  break;
               }
            }
            readBytes += localReadBytes;
            index += localReadBytes;
            length -= localReadBytes;
         }
         while (length > 0);
      }
      else
      {
         byte[] tmp = new byte[length];
         int i = 0;
         do
         {
            int localReadBytes = in.read(tmp, i, tmp.length - i);
            if (localReadBytes < 0)
            {
               if (readBytes == 0)
               {
                  return -1;
               }
               else
               {
                  break;
               }
            }
            readBytes += localReadBytes;
            i += readBytes;
         }
         while (i < tmp.length);
         ((ByteBuffer)buffer.duplicate().position(index)).put(tmp);
      }

      return readBytes;
   }

   public int setBytes(final int index, final ScatteringByteChannel in, final int length) throws IOException
   {

      ByteBuffer slice = (ByteBuffer)buffer.duplicate().limit(index + length).position(index);
      int readBytes = 0;

      while (readBytes < length)
      {
         int localReadBytes;
         try
         {
            localReadBytes = in.read(slice);
         }
         catch (ClosedChannelException e)
         {
            localReadBytes = -1;
         }
         if (localReadBytes < 0)
         {
            if (readBytes == 0)
            {
               return -1;
            }
            else
            {
               return readBytes;
            }
         }
         else if (localReadBytes == 0)
         {
            break;
         }
         readBytes += localReadBytes;
      }

      return readBytes;
   }

   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      if (index == 0 && length == capacity())
      {
         return buffer.duplicate();
      }
      else
      {
         return ((ByteBuffer)buffer.duplicate().position(index).limit(index + length)).slice();
      }
   }

   @Override
   public ByteBuffer toByteBuffer()
   {
      return buffer;
   }

   public String toString(final int index, final int length, final String charsetName)
   {
      if (!buffer.isReadOnly() && buffer.hasArray())
      {
         try
         {
            return new String(buffer.array(), index + buffer.arrayOffset(), length, charsetName);
         }
         catch (UnsupportedEncodingException e)
         {
            throw new UnsupportedCharsetException(charsetName);
         }
      }
      else
      {
         byte[] tmp = new byte[length];
         ((ByteBuffer)buffer.duplicate().position(index)).get(tmp);
         try
         {
            return new String(tmp, charsetName);
         }
         catch (UnsupportedEncodingException e)
         {
            throw new UnsupportedCharsetException(charsetName);
         }
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.buffers.ChannelBuffer#array()
    */
   public byte[] array()
   {
      return buffer.array();
   }
   
   public HornetQBuffer copy()
   {
      return new HornetQByteBufferBackedChannelBuffer(ByteBuffer.wrap(buffer.array().clone()));
   }
}
