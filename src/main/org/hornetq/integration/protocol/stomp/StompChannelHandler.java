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

package org.hornetq.integration.protocol.stomp;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.integration.transports.netty.AbstractServerChannelHandler;
import org.hornetq.integration.transports.netty.NettyAcceptor;
import org.hornetq.integration.transports.netty.ServerHolder;
import org.hornetq.jms.client.HornetQBytesMessage;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.UUIDGenerator;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;

@ChannelPipelineCoverage("one")
public final class StompChannelHandler extends AbstractServerChannelHandler
{
   static final Logger log = Logger.getLogger(StompChannelHandler.class);

   final StompMarshaller marshaller;

   private final Map<RemotingConnection, ServerSession> sessions = new HashMap<RemotingConnection, ServerSession>();

   private ServerHolder serverHandler;

   public StompChannelHandler(ServerHolder serverHolder,
                              final ChannelGroup group,
                              NettyAcceptor acceptor,
                              final ConnectionLifeCycleListener listener)
   {
      super(group, listener, acceptor);
      this.serverHandler = serverHolder;
      this.marshaller = new StompMarshaller();
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
            response = new StompFrame(Stomp.Responses.ERROR,
                                      new HashMap<String, Object>(),
                                      ("Unsupported frame: " + command).getBytes());
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
         stream.close();

         Map<String, Object> headers = new HashMap<String, Object>();
         headers.put(Stomp.Headers.Error.MESSAGE, e.getMessage());

         final String receiptId = (String)frame.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
         if (receiptId != null)
         {
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
   private StompFrame onSubscribe(StompFrame frame, HornetQServer server, RemotingConnection connection) throws Exception,
                                                                                                        StompException,
                                                                                                        HornetQException
   {
      Map<String, Object> headers = frame.getHeaders();
      String queue = (String)headers.get(Stomp.Headers.Send.DESTINATION);
      SimpleString queueName = StompDestinationConverter.toHornetQAddress(queue);

      ServerSession session = checkAndGetSession(connection);
      long consumerID = server.getStorageManager().generateUniqueID();
      session.createConsumer(consumerID, queueName, null, false);
      session.receiveConsumerCredits(consumerID, -1);
      session.start();

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

   private StompFrame onSend(StompFrame frame, HornetQServer server, RemotingConnection connection) throws Exception
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
      SimpleString address = StompDestinationConverter.toHornetQAddress(queue);

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

      session.send(message);
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

   private StompFrame onConnect(StompFrame frame, HornetQServer server, final RemotingConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String login = (String)headers.get(Stomp.Headers.Connect.LOGIN);
      String passcode = (String)headers.get(Stomp.Headers.Connect.PASSCODE);
      String requestID = (String)headers.get(Stomp.Headers.Connect.REQUEST_ID);

      String name = UUIDGenerator.getInstance().generateStringUUID();
      server.createSession(name,
                           login,
                           passcode,
                           HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                           connection,
                           true,
                           true,
                           false,
                           false,
                           new StompSessionCallback(marshaller, connection));
      ServerSession session = server.getSession(name);
      sessions.put(connection, session);
      System.out.println(">>> created session " + session);
      HashMap<String, Object> h = new HashMap<String, Object>();
      h.put(Stomp.Headers.Connected.SESSION, name);
      h.put(Stomp.Headers.Connected.RESPONSE_ID, requestID);
      return new StompFrame(Stomp.Responses.CONNECTED, h, new byte[] {});
   }
}