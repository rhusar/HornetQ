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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.ProtocolType;
import org.hornetq.core.remoting.impl.ssl.SSLSupport;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.integration.protocol.stomp.StompChannelHandler;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.VersionLoader;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.util.Version;
import org.jboss.netty.util.VirtualExecutorService;

/**
 * A Netty TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @version $Rev$, $Date$
 */
public class NettyAcceptor implements Acceptor
{
   static final Logger log = Logger.getLogger(NettyAcceptor.class);

   private ChannelFactory channelFactory;

   private volatile ChannelGroup serverChannelGroup;

   private volatile ChannelGroup channelGroup;

   private ServerBootstrap bootstrap;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean httpEnabled;

   private final long httpServerScanPeriod;

   private final long httpResponseTime;

   private final boolean useNio;

   private final boolean useInvm;

   private final String protocol;

   private final String host;

   private final int port;

   private final String keyStorePath;

   private final String keyStorePassword;

   private final String trustStorePath;

   private final String trustStorePassword;

   private final boolean tcpNoDelay;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   private final HttpKeepAliveRunnable httpKeepAliveRunnable;

   private final ConcurrentMap<Object, Connection> connections = new ConcurrentHashMap<Object, Connection>();

   private final Executor threadPool;

   private NotificationService notificationService;

   private VirtualExecutorService bossExecutor;

   private ServerHolder serverHandler;

   public NettyAcceptor(final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final ServerHolder serverHandler,
                        final ConnectionLifeCycleListener listener,
                        final Executor threadPool,
                        final ScheduledExecutorService scheduledThreadPool)
   {
      this.handler = handler;

      this.serverHandler = serverHandler;

      this.listener = listener;

      sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME,
                                                          TransportConstants.DEFAULT_SSL_ENABLED,
                                                          configuration);

      httpEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_ENABLED_PROP_NAME,
                                                           TransportConstants.DEFAULT_HTTP_ENABLED,
                                                           configuration);

      if (httpEnabled)
      {
         httpServerScanPeriod = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME,
                                                                    TransportConstants.DEFAULT_HTTP_SERVER_SCAN_PERIOD,
                                                                    configuration);
         httpResponseTime = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME,
                                                                TransportConstants.DEFAULT_HTTP_RESPONSE_TIME,
                                                                configuration);
         httpKeepAliveRunnable = new HttpKeepAliveRunnable();
         Future<?> future = scheduledThreadPool.scheduleAtFixedRate(httpKeepAliveRunnable,
                                                                    httpServerScanPeriod,
                                                                    httpServerScanPeriod,
                                                                    TimeUnit.MILLISECONDS);
         httpKeepAliveRunnable.setFuture(future);
      }
      else
      {
         httpServerScanPeriod = 0;
         httpResponseTime = 0;
         httpKeepAliveRunnable = null;
      }
      useNio = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_PROP_NAME,
                                                      TransportConstants.DEFAULT_USE_NIO_SERVER,
                                                      configuration);

      useInvm = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_INVM_PROP_NAME,
                                                       TransportConstants.DEFAULT_USE_INVM,
                                                       configuration);
      protocol = ConfigurationHelper.getStringProperty(TransportConstants.PROTOCOL_PROP_NAME,
                                                       TransportConstants.DEFAULT_PROTOCOL,
                                                       configuration);
      host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME,
                                                   TransportConstants.DEFAULT_HOST,
                                                   configuration);
      port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME,
                                                TransportConstants.DEFAULT_PORT,
                                                configuration);
      if (sslEnabled)
      {
         keyStorePath = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME,
                                                              TransportConstants.DEFAULT_KEYSTORE_PATH,
                                                              configuration);
         keyStorePassword = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME,
                                                                  TransportConstants.DEFAULT_KEYSTORE_PASSWORD,
                                                                  configuration);
         trustStorePath = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,
                                                                TransportConstants.DEFAULT_TRUSTSTORE_PATH,
                                                                configuration);
         trustStorePassword = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME,
                                                                    TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD,
                                                                    configuration);
      }
      else
      {
         keyStorePath = null;
         keyStorePassword = null;
         trustStorePath = null;
         trustStorePassword = null;
      }

      tcpNoDelay = ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME,
                                                          TransportConstants.DEFAULT_TCP_NODELAY,
                                                          configuration);
      tcpSendBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME,
                                                             TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE,
                                                             configuration);
      tcpReceiveBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME,
                                                                TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE,
                                                                configuration);

      this.threadPool = threadPool;
   }

   public synchronized void start() throws Exception
   {
      if (channelFactory != null)
      {
         // Already started
         return;
      }

      bossExecutor = new VirtualExecutorService(threadPool);
      VirtualExecutorService workerExecutor = new VirtualExecutorService(threadPool);

      if (useInvm)
      {
         channelFactory = new DefaultLocalServerChannelFactory();
      }
      else if (useNio)
      {
         channelFactory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor);
      }
      else
      {
         channelFactory = new OioServerSocketChannelFactory(bossExecutor, workerExecutor);
      }
      bootstrap = new ServerBootstrap(channelFactory);

      final SSLContext context;
      if (sslEnabled)
      {
         try
         {
            context = SSLSupport.createServerContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword);
         }
         catch (Exception e)
         {
            IllegalStateException ise = new IllegalStateException("Unable to create NettyAcceptor for " + host +
                                                                  ":" +
                                                                  port);
            ise.initCause(e);
            throw ise;
         }
      }
      else
      {
         context = null; // Unused
      }

      ChannelPipelineFactory factory = new ChannelPipelineFactory()
      {
         public ChannelPipeline getPipeline() throws Exception
         {
            ChannelPipeline pipeline = Channels.pipeline();
            if (sslEnabled)
            {
               ChannelPipelineSupport.addSSLFilter(pipeline, context, false);
            }
            if (httpEnabled)
            {
               pipeline.addLast("httpRequestDecoder", new HttpRequestDecoder());
               pipeline.addLast("httpResponseEncoder", new HttpResponseEncoder());
               pipeline.addLast("httphandler", new HttpAcceptorHandler(httpKeepAliveRunnable, httpResponseTime));
            }
            if (protocol.equals(TransportConstants.STOMP_PROTOCOL))
            {
               ChannelPipelineSupport.addStompStack(pipeline, serverHandler);
               pipeline.addLast("handler", new StompChannelHandler(serverHandler,
                                                                   channelGroup,
                                                                   NettyAcceptor.this,
                                                                   new Listener()));
            }
            else
            {
               ChannelPipelineSupport.addHornetQCodecFilter(pipeline, handler);
               pipeline.addLast("handler", new HornetQServerChannelHandler(channelGroup, handler, new Listener()));
            }

            return pipeline;
         }
      };
      bootstrap.setPipelineFactory(factory);

      // Bind
      bootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
      if (tcpReceiveBufferSize != -1)
      {
         bootstrap.setOption("child.receiveBufferSize", tcpReceiveBufferSize);
      }
      if (tcpSendBufferSize != -1)
      {
         bootstrap.setOption("child.sendBufferSize", tcpSendBufferSize);
      }
      bootstrap.setOption("reuseAddress", true);
      bootstrap.setOption("child.reuseAddress", true);
      bootstrap.setOption("child.keepAlive", true);

      channelGroup = new DefaultChannelGroup("hornetq-accepted-channels");

      serverChannelGroup = new DefaultChannelGroup("hornetq-acceptor-channels");

      startServerChannels();

      paused = false;

      if (!Version.ID.equals(VersionLoader.getVersion().getNettyVersion()))
      {
         NettyAcceptor.log.warn("Unexpected Netty Version was expecting " + VersionLoader.getVersion()
                                                                                         .getNettyVersion() +
                                " using " +
                                Version.ID);
      }

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("factory"),
                                       new SimpleString(NettyAcceptorFactory.class.getName()));
         props.putSimpleStringProperty(new SimpleString("host"), new SimpleString(host));
         props.putIntProperty(new SimpleString("port"), port);
         Notification notification = new Notification(null, NotificationType.ACCEPTOR_STARTED, props);
         notificationService.sendNotification(notification);
      }

      NettyAcceptor.log.info("Started Netty Acceptor version " + Version.ID);
   }

   private void startServerChannels()
   {
      String[] hosts = TransportConfiguration.splitHosts(host);
      for (String h : hosts)
      {
         SocketAddress address;
         if (useInvm)
         {
            address = new LocalAddress(h);
         }
         else
         {
            address = new InetSocketAddress(h, port);
         }
         Channel serverChannel = bootstrap.bind(address);
         serverChannelGroup.add(serverChannel);
      }
   }

   public synchronized void stop()
   {
      if (channelFactory == null)
      {
         return;
      }

      serverChannelGroup.close().awaitUninterruptibly();

      if (httpKeepAliveRunnable != null)
      {
         httpKeepAliveRunnable.close();
      }

      // serverChannelGroup has been unbound in pause()
      serverChannelGroup.close().awaitUninterruptibly();
      ChannelGroupFuture future = channelGroup.close().awaitUninterruptibly();

      if (!future.isCompleteSuccess())
      {
         NettyAcceptor.log.warn("channel group did not completely close");
         Iterator<Channel> iterator = future.getGroup().iterator();
         while (iterator.hasNext())
         {
            Channel channel = iterator.next();
            if (channel.isBound())
            {
               NettyAcceptor.log.warn(channel + " is still connected to " + channel.getRemoteAddress());
            }
         }
      }

      channelFactory.releaseExternalResources();
      channelFactory = null;

      for (Connection connection : connections.values())
      {
         listener.connectionDestroyed(connection.getID());
      }

      connections.clear();

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("factory"),
                                       new SimpleString(NettyAcceptorFactory.class.getName()));
         props.putSimpleStringProperty(new SimpleString("host"), new SimpleString(host));
         props.putIntProperty(new SimpleString("port"), port);
         Notification notification = new Notification(null, NotificationType.ACCEPTOR_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }

      paused = false;
   }

   public boolean isStarted()
   {
      return channelFactory != null;
   }

   private boolean paused;

   public void pause()
   {
      if (paused)
      {
         return;
      }

      if (channelFactory == null)
      {
         return;
      }

      // We *pause* the acceptor so no new connections are made
      ChannelGroupFuture future = serverChannelGroup.unbind().awaitUninterruptibly();
      if (!future.isCompleteSuccess())
      {
         NettyAcceptor.log.warn("server channel group did not completely unbind");
         Iterator<Channel> iterator = future.getGroup().iterator();
         while (iterator.hasNext())
         {
            Channel channel = iterator.next();
            if (channel.isBound())
            {
               NettyAcceptor.log.warn(channel + " is still bound to " + channel.getRemoteAddress());
            }
         }
      }
      // TODO remove workaround when integrating Netty 3.2.x
      // https://jira.jboss.org/jira/browse/NETTY-256
      bossExecutor.shutdown();
      try
      {

         bossExecutor.awaitTermination(30, TimeUnit.SECONDS);
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
      }

      paused = true;
   }

   public void setNotificationService(final NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }

   public ConnectionLifeCycleListener newListener()
   {
      return new Listener();
   }

   // Inner classes -----------------------------------------------------------------------------

   private final class HornetQServerChannelHandler extends AbstractServerChannelHandler
   {
      private BufferHandler handler;

      HornetQServerChannelHandler(final ChannelGroup group,
                                  final BufferHandler handler,
                                  final ConnectionLifeCycleListener listener)
      {
         super(group, listener, NettyAcceptor.this);

         this.handler = handler;
      }

      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
      {
         ChannelBuffer buffer = (ChannelBuffer)e.getMessage();

         handler.bufferReceived(e.getChannel().getId(), new ChannelBufferWrapper(buffer));
      }

   }

   class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(final Connection connection, final ProtocolType protocol)
      {
         if (connections.putIfAbsent(connection.getID(), connection) != null)
         {
            throw new IllegalArgumentException("Connection already exists with id " + connection.getID());
         }

         listener.connectionCreated(connection, protocol);
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (connections.remove(connectionID) != null)
         {
            listener.connectionDestroyed(connectionID);
         }
      }

      public void connectionException(final Object connectionID, final HornetQException me)
      {
         // Execute on different thread to avoid deadlocks
         new Thread()
         {
            @Override
            public void run()
            {
               listener.connectionException(connectionID, me);
            }
         }.start();

      }
   }

}
