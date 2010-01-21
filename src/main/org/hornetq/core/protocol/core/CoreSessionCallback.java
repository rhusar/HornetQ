package org.hornetq.core.protocol.core;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.protocol.core.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.wireformat.SessionReceiveMessage;
import org.hornetq.core.remoting.server.ProtocolManager;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.SessionCallback;

/**
 * A CoreSessionCallback
 *
 * @author jmesnil
 *
 *
 */
public final class CoreSessionCallback implements SessionCallback
{
   private final Channel channel;

   private ProtocolManager protocolManager;

   private String name;

   public CoreSessionCallback(String name, ProtocolManager protocolManager, Channel channel)
   {
      this.name = name;
      this.protocolManager = protocolManager;
      this.channel = channel;
   }

   public int sendLargeMessage(long consumerID, byte[] headerBuffer, long bodySize, int deliveryCount)
   {
      Packet packet = new SessionReceiveLargeMessage(consumerID, headerBuffer, bodySize, deliveryCount);

      channel.send(packet);

      return packet.getPacketSize();
   }

   public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
   {
      Packet packet = new SessionReceiveContinuationMessage(consumerID, body, continues, requiresResponse);

      channel.send(packet);

      return packet.getPacketSize();
   }

   public int sendMessage(ServerMessage message, long consumerID, int deliveryCount)
   {
      Packet packet = new SessionReceiveMessage(consumerID, message, deliveryCount);

      channel.send(packet);

      return packet.getPacketSize();
   }

   public void sendProducerCreditsMessage(int credits, SimpleString address, int offset)
   {
      Packet packet = new SessionProducerCreditsMessage(credits, address, offset);

      channel.send(packet);
   }

   public void closed()
   {
      protocolManager.removeHandler(name);
   }
}