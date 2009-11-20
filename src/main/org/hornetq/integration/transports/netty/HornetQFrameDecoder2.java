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

package org.hornetq.integration.transports.netty;

import static org.hornetq.utils.DataConstants.SIZE_INT;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.hornetq.core.remoting.impl.AbstractBufferHandler;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;

/**
 * A Netty decoder used to decode messages.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 *
 * @version $Revision: 7839 $, $Date: 2009-08-21 02:26:39 +0900 (2009-08-21, 금) $
 */
@ChannelPipelineCoverage("one")
public class HornetQFrameDecoder2 extends SimpleChannelUpstreamHandler
{
   private ChannelBuffer previousData = ChannelBuffers.EMPTY_BUFFER;

   // SimpleChannelUpstreamHandler overrides
   // -------------------------------------------------------------------------------------

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
   {
      ChannelBuffer in = (ChannelBuffer) e.getMessage();
      if (previousData.readable())
      {
         if (previousData.readableBytes() + in.readableBytes() < SIZE_INT) {
            append(in, 512); // Length is unknown. Bet at 512.
            return;
         }
         
         // Decode the first message.  The first message requires a special
         // treatment because it is the only message that spans over the two
         // buffers.
         final int length;
         switch (previousData.readableBytes()) {
            case 1:
               length = (previousData.getUnsignedByte(previousData.readerIndex()) << 24) |
                        in.getMedium(in.readerIndex());
               if (in.readableBytes() - 3 < length) {
                  append(in, length);
                  return;
               }
               break;
            case 2:
               length = (previousData.getUnsignedShort(previousData.readerIndex()) << 16) |
                        in.getUnsignedShort(in.readerIndex());
               if (in.readableBytes() - 2 < length) {
                  append(in, length);
                  return;
               }
               break;
            case 3:
               length = (previousData.getUnsignedMedium(previousData.readerIndex()) << 8) |
                        in.getUnsignedByte(in.readerIndex());
               if (in.readableBytes() - 1 < length) {
                  append(in, length);
                  return;
               }
               break;
            case 4:
               length = previousData.getInt(previousData.readerIndex());
               if (in.readableBytes() - 4 < length) {
                  append(in, length);
                  return;
               }
               break;
            default:
               length = previousData.getInt(previousData.readerIndex());
               if (in.readableBytes() + previousData.readableBytes() - 4 < length) {
                  append(in, length);
                  return;
               }
         }
         
         final ChannelBuffer frame;
         if (previousData instanceof DynamicChannelBuffer) {
            // It's safe to reuse the current dynamic buffer
            // because previousData will be reassigned to
            // EMPTY_BUFFER or 'in' later.
            previousData.writeBytes(in, length + 4 - previousData.readableBytes());
            frame = previousData;
         } else {
            frame = ChannelBuffers.dynamicBuffer(length + 4);
            frame.writeBytes(previousData, previousData.readerIndex(), previousData.readableBytes());
            frame.writeBytes(in, length + 4 - frame.writerIndex());
         }

         frame.skipBytes(4);
         Channels.fireMessageReceived(ctx, frame);

         if (!in.readable()) {
            previousData = ChannelBuffers.EMPTY_BUFFER;
            return;
         }
      }

      // And then handle the rest - we don't need to deal with the
      // composite buffer anymore because the second or later messages
      // always belong to the second buffer.
      decode(ctx, in);
      
      // Handle the leftover.
      if (in.readable())
      {
         previousData = in;
      }
      else
      {
         previousData = ChannelBuffers.EMPTY_BUFFER;
      }
   }
   
   private void decode(ChannelHandlerContext ctx, ChannelBuffer in)
   {
      for (;;) {
         final int readableBytes = in.readableBytes();
         if (readableBytes < SIZE_INT) {
            break;
         }
         
         final int length = in.getInt(in.readerIndex());
         if (readableBytes < length + SIZE_INT) {
            break;
         }
         
         // Convert to dynamic buffer (this requires copy)
         ChannelBuffer frame = ChannelBuffers.dynamicBuffer(length + SIZE_INT);
         frame.writeBytes(in, length + SIZE_INT);
         frame.skipBytes(SIZE_INT);
         Channels.fireMessageReceived(ctx, frame);
      }
   }
   
   private void append(ChannelBuffer in, int length)
   {
      // Need more data to decode the first message. This can happen when
      // a client is very slow. (e.g.sending each byte one by one)
      if (previousData instanceof DynamicChannelBuffer)
      {
         previousData.discardReadBytes();
         previousData.writeBytes(in);
      }
      else
      {
         ChannelBuffer newPreviousData =
              ChannelBuffers.dynamicBuffer(
                    Math.max(previousData.readableBytes() + in.readableBytes(), length + 4));
         newPreviousData.writeBytes(previousData);
         newPreviousData.writeBytes(in);
         previousData = newPreviousData;
      }
   }
}
