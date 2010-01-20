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

package org.hornetq.integration.stomp;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * A StompPacketDecoder
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
@ChannelPipelineCoverage("one")
public class StompPacketDecoder extends SimpleChannelHandler
{
   private final StompMarshaller marshaller;
   
   // PacketDecoder implementation ----------------------------------

   public StompPacketDecoder(final StompMarshaller marshaller)
   {
      this.marshaller = marshaller;
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
   {
      ChannelBuffer in = (ChannelBuffer)e.getMessage();
      HornetQBuffer buffer = new ChannelBufferWrapper(in);
      StompFrame frame = marshaller.unmarshal(buffer);
      
      Channels.fireMessageReceived(ctx, frame);
   }
}
