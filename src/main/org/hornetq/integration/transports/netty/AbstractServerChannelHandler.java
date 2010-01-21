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

package org.hornetq.integration.transports.netty;

import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.ssl.SslHandler;

@ChannelPipelineCoverage("one")
public abstract class AbstractServerChannelHandler extends HornetQChannelHandler
{
   private NettyAcceptor acceptor;

   protected AbstractServerChannelHandler(final ChannelGroup group, final ConnectionLifeCycleListener listener, NettyAcceptor acceptor)
   {
      super(group, listener);
      this.acceptor = acceptor;
   }

   @Override
   public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
   {
      new NettyConnection(e.getChannel(), acceptor.newListener());

      SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
      if (sslHandler != null)
      {
         sslHandler.handshake(e.getChannel()).addListener(new ChannelFutureListener()
         {
            public void operationComplete(final ChannelFuture future) throws Exception
            {
               if (future.isSuccess())
               {
                  active = true;
               }
               else
               {
                  future.getChannel().close();
               }
            }
         });
      }
      else
      {
         active = true;
      }
   }
}