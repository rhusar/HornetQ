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

import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * A ReplicationLargeMessageBeingMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationLargeMessageBeingMessage extends PacketImpl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   byte header[];

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationLargeMessageBeingMessage(final byte[] header)
   {
      this();
      this.header = header;
   }

   public ReplicationLargeMessageBeingMessage()
   {
      super(REPLICATION_LARGE_MESSAGE_BEGIN);
   }

   // Public --------------------------------------------------------

   @Override
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_INT + header.length;
   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeInt(header.length);
      buffer.writeBytes(header);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      int size = buffer.readInt();
      header = new byte[size];
      buffer.readBytes(header);
   }

   /**
    * @return the header
    */
   public byte[] getHeader()
   {
      return header;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
