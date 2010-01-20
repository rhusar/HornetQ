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

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.PacketDecoder;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.CorePacketDecoder;
import org.hornetq.core.remoting.impl.ssl.SSLSupport;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.integration.stomp.Stomp;
import org.hornetq.integration.stomp.StompDestinationConverter;
import org.hornetq.integration.stomp.StompException;
import org.hornetq.integration.stomp.StompFrame;
import org.hornetq.integration.stomp.StompMarshaller;
import org.hornetq.jms.client.HornetQBytesMessage;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUIDGenerator;
import org.hornetq.utils.VersionLoader;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
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
import org.jboss.netty.handler.ssl.SslHandler;
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
   private static final Logger log = Logger.getLogger(NettyAcceptor.class);

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
                                                                   new StompMarshaller(),
                                                                   channelGroup,
                                                                   new Listener()));
            }
            else
            {
               ChannelPipelineSupport.addHornetQCodecFilter(pipeline, handler);
               PacketDecoder decoder = new CorePacketDecoder();
               pipeline.addLast("handler", new HornetQServerChannelHandler(channelGroup,
                                                                           decoder,
                                                                           handler,
                                                                           new Listener()));
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

   // Inner classes -----------------------------------------------------------------------------

   private final class HornetQServerChannelHandler extends AbstractServerChannelHandler
   {
      private PacketDecoder decoder;

      private BufferHandler handler;

      HornetQServerChannelHandler(final ChannelGroup group,
                                  final PacketDecoder decoder,
                                  final BufferHandler handler,
                                  final ConnectionLifeCycleListener listener)
      {
         super(group, listener);

         this.decoder = decoder;
         this.handler = handler;
      }

      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
      {
         ChannelBuffer buffer = (ChannelBuffer)e.getMessage();

         handler.bufferReceived(e.getChannel().getId(), new ChannelBufferWrapper(buffer), decoder);
      }

   }

   @ChannelPipelineCoverage("one")
   public final class StompChannelHandler extends AbstractServerChannelHandler
   {
      private final StompMarshaller marshaller;

      private final Map<RemotingConnection, ServerSession> sessions = new HashMap<RemotingConnection, ServerSession>();

      private ServerHolder serverHandler;

      public StompChannelHandler(ServerHolder serverHolder,
                                 StompMarshaller marshaller,
                                 final ChannelGroup group,
                                 final ConnectionLifeCycleListener listener)
      {
         super(group, listener);
         this.serverHandler = serverHolder;
         this.marshaller = marshaller;
      }

      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
      {
         StompFrame frame = (StompFrame)e.getMessage();
         System.out.println(">>> got frame " + frame);

         // need to interact with HornetQ server & session
         HornetQServer server = serverHandler.getServer();
         RemotingConnection connection = serverHandler.getRemotingConnection(e.getChannel().getId());

         try
         {

            String command = frame.getCommand();

            StompFrame response = null;
            if (Stomp.Commands.CONNECT.equals(command))
            {
               response = onConnect(frame, server, connection);
            }
            else if (Stomp.Commands.DISCONNECT.equals(command))
            {
               response = onDisconnect(frame, server, connection);
            }
            else if (Stomp.Commands.SEND.equals(command))
            {
               response = onSend(frame, server, connection);
            }
            else if (Stomp.Commands.SUBSCRIBE.equals(command))
            {
               response = onSubscribe(frame, server, connection);
            }
            else
            {
               log.error("Unsupported Stomp frame: " + frame);
               response = new StompFrame(Stomp.Responses.ERROR, new HashMap<String, Object>(), ("Unsupported frame: " + command).getBytes());
            }
            
            if (response != null)
            {
               System.out.println(">>> will reply " + response);
               byte[] bytes = marshaller.marshal(response);
               HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(bytes);
               System.out.println("ready to send reply: " + buffer);
               connection.getTransportConnection().write(buffer, true);
            }
         }
         catch (StompException ex)
         {
            // Let the stomp client know about any protocol errors.
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos, "UTF-8"));
            ex.printStackTrace(stream);
            stream.append(Stomp.NULL + Stomp.NEWLINE);
            stream.close();

            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put(Stomp.Headers.Error.MESSAGE, e.getMessage());

            final String receiptId = (String) frame.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
            if (receiptId != null) {
                headers.put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
            }

            StompFrame errorMessage = new StompFrame(Stomp.Responses.ERROR, headers, baos.toByteArray());
            byte[] bytes = marshaller.marshal(errorMessage);
            HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(bytes);
            System.out.println("ready to send reply: " + buffer);
            connection.getTransportConnection().write(buffer, true);

         }
         catch (Exception ex)
         {
            ex.printStackTrace();
         }
      }

      /**
       * @param frame
       * @param server
       * @param connection
       * @return
       * @throws StompException 
       * @throws HornetQException 
       */
      private StompFrame onSubscribe(StompFrame frame, HornetQServer server, RemotingConnection connection) throws StompException, HornetQException
      {
         Map<String, Object> headers = frame.getHeaders();
         String queue = (String)headers.get(Stomp.Headers.Send.DESTINATION);
         SimpleString queueName = StompDestinationConverter.convertDestination(queue);

         ServerSession session = checkAndGetSession(connection);
         long id = server.getStorageManager().generateUniqueID();
         SessionCreateConsumerMessage packet = new SessionCreateConsumerMessage(id , queueName, null, false, false);
         session.handleCreateConsumer(packet);
         SessionConsumerFlowCreditMessage credits = new SessionConsumerFlowCreditMessage(id, -1);
         session.handleReceiveConsumerCredits(credits );
         session.handleStart(new PacketImpl(PacketImpl.SESS_START));

         return null;
      }

      private ServerSession checkAndGetSession(RemotingConnection connection) throws StompException
      {
         ServerSession session = sessions.get(connection);
         if (session == null)
         {
            throw new StompException("Not connected");
         }
         return session;
      }
      
      private StompFrame onDisconnect(StompFrame frame, HornetQServer server, RemotingConnection connection) throws StompException
      {
         ServerSession session = checkAndGetSession(connection);
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (Exception e)
            {
               throw new StompException(e.getMessage());
            }
            sessions.remove(connection);
         }
         return null;
      }

      private StompFrame onSend(StompFrame frame, HornetQServer server, RemotingConnection connection) throws HornetQException, StompException
      {
         ServerSession session = checkAndGetSession(connection);
         
         Map<String, Object> headers = frame.getHeaders();
         String queue = (String)headers.get(Stomp.Headers.Send.DESTINATION);
         /*
         String type = (String)headers.get(Stomp.Headers.Send.TYPE);
         long expiration = (Long)headers.get(Stomp.Headers.Send.EXPIRATION_TIME);
         byte priority = (Byte)headers.get(Stomp.Headers.Send.PRIORITY);
         boolean durable = (Boolean)headers.get(Stomp.Headers.Send.PERSISTENT);
         */
         byte type = HornetQTextMessage.TYPE;
         if (headers.containsKey(Stomp.Headers.CONTENT_LENGTH))
         {
            type = HornetQBytesMessage.TYPE;
         }
         long timestamp = System.currentTimeMillis();
         boolean durable = false;
         long expiration = -1;
         byte priority = 9;
         SimpleString address = StompDestinationConverter.convertDestination(queue);

         ServerMessage message = new ServerMessageImpl(server.getStorageManager().generateUniqueID(), 512);
         message.setType(type);
         message.setTimestamp(timestamp);
         message.setAddress(address);
         byte[] content = frame.getContent();
         if (type == HornetQTextMessage.TYPE)
         {
            message.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString(new String(content)));
         }
         else
         {
            message.getBodyBuffer().writeBytes(content);
         }

         SessionSendMessage packet = new SessionSendMessage(message, false);
         session.handleSend(packet);
         if (headers.containsKey(Stomp.Headers.RECEIPT_REQUESTED))
         {
            Map<String, Object> h = new HashMap<String, Object>();
            h.put(Stomp.Headers.Response.RECEIPT_ID, headers.get(Stomp.Headers.RECEIPT_REQUESTED));
            return new StompFrame(Stomp.Responses.RECEIPT, h, new byte[] {});
         }
         else
         {
            return null;
         }
      }

      private StompFrame onConnect(StompFrame frame, HornetQServer server, RemotingConnection connection) throws Exception
      {
         Map<String, Object> headers = frame.getHeaders();
         String login = (String)headers.get(Stomp.Headers.Connect.LOGIN);
         String passcode = (String)headers.get(Stomp.Headers.Connect.PASSCODE);
         String requestID = (String)headers.get(Stomp.Headers.Connect.REQUEST_ID);

         String name = UUIDGenerator.getInstance().generateStringUUID();
         server.createSession(name,
                              1,
                              login,
                              passcode,
                              HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                              VersionLoader.getVersion().getIncrementingVersion(),
                              connection,
                              true,
                              true,
                              false,
                              false,
                              -1);
         ServerSession session = server.getSession(name);
         sessions.put(connection, session);
         System.out.println(">>> created session " + session);
         HashMap<String, Object> h = new HashMap<String, Object>();
         h.put(Stomp.Headers.Connected.SESSION, name);
         h.put(Stomp.Headers.Connected.RESPONSE_ID, requestID);
         return new StompFrame(Stomp.Responses.CONNECTED, h, new byte[] {});
      }
   }

   @ChannelPipelineCoverage("one")
   public abstract class AbstractServerChannelHandler extends HornetQChannelHandler
   {
      protected AbstractServerChannelHandler(final ChannelGroup group, final ConnectionLifeCycleListener listener)
      {
         super(group, listener);
      }

      @Override
      public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception
      {
         new NettyConnection(e.getChannel(), new Listener());

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

   private class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(final Connection connection)
      {
         if (connections.putIfAbsent(connection.getID(), connection) != null)
         {
            throw new IllegalArgumentException("Connection already exists with id " + connection.getID());
         }

         listener.connectionCreated(connection);
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
