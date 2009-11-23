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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.message.Message;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.utils.DataConstants;

/**
 * A MessagePacket
 *
 * @author tim
 *
 *
 */
public abstract class MessagePacket extends PacketImpl
{
   protected Message message;
      
   public MessagePacket(final byte type, final Message message)
   {
      super(type);
      
      this.message = message;
   }
   
   public Message getMessage()
   {
      return message;
   }
   
   @Override
   public HornetQBuffer encode(final RemotingConnection connection)
   {
      HornetQBuffer buffer = message.encodeToBuffer();
      
      buffer.setIndex(0, message.getEndOfMessagePosition());
      
      encodeExtraData(buffer);
      
      size = buffer.writerIndex();
                       
      //Write standard headers
      
      int len = size - DataConstants.SIZE_INT;
      buffer.setInt(0, len);
      buffer.setByte(DataConstants.SIZE_INT, type);
      buffer.setLong(DataConstants.SIZE_INT + DataConstants.SIZE_BYTE, channelID);
      
      //Position reader for reading by Netty
      buffer.readerIndex(0);
      
      return buffer;
   }
   
   @Override
   public void decodeRest(HornetQBuffer buffer)
   {
      //Buffer comes in after having read standard headers and positioned at Beginning of body part
      
      message.decodeFromBuffer(buffer);
      
      decodeExtraData(buffer);      
   }
   
   protected abstract void encodeExtraData(HornetQBuffer buffer);
   
   protected abstract void decodeExtraData(HornetQBuffer buffer);
}
