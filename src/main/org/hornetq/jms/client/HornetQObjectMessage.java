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

package org.hornetq.jms.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;

/**
 * This class implements javax.jms.ObjectMessage
 * 
 * Don't used ObjectMessage if you want good performance!
 * 
 * Java Serialization is slooooow!
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version $Revision: 3412 $
 *
 * $Id: HornetQRAObjectMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class HornetQObjectMessage extends HornetQMessage implements ObjectMessage
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(HornetQObjectMessage.class);

   public static final byte TYPE = 2;

   // Attributes ----------------------------------------------------

   private Serializable object;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQObjectMessage(final ClientSession session)
   {
      super(HornetQObjectMessage.TYPE, session);
   }

   public HornetQObjectMessage(final ClientMessage message, ClientSession session)
   {
      super(message, session);
   }

   /**
    * A copy constructor for foreign JMS ObjectMessages.
    */
   public HornetQObjectMessage(final ObjectMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQObjectMessage.TYPE, session);

      setObject(foreign.getObject());
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return HornetQObjectMessage.TYPE;
   }

   public void doBeforeSend() throws Exception
   {
      super.doBeforeSend();
   }

   public void doBeforeReceive() throws Exception
   {
      super.doBeforeReceive();
      
      HornetQBuffer buffer = message.getBuffer();
      
      byte[] bytes = new byte[buffer.writerIndex() - buffer.readerIndex()];
      
      buffer.readBytes(bytes);

      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));

      object = (Serializable)ois.readObject();
   }

   // ObjectMessage implementation ----------------------------------

   public void setObject(Serializable object) throws JMSException
   {
      checkWrite();

      this.object = object;

//     This is actually slower than serializing into a byte[] first
//      try
//      {
//         ObjectOutputStream oos = new ObjectOutputStream(new BufferOutputStream(message.getBuffer()));
//
//         oos.writeObject(object);
//
//         oos.flush();
//      }
//      catch (IOException e)
//      {
//         log.error("Failed to serialise object", e);
//      }
      
      //It's actually faster to serialize into a ByteArrayOutputStream than direct into the buffer
      try
      {
         ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         
         ObjectOutputStream oos = new ObjectOutputStream(baos);

         oos.writeObject(object);

         oos.flush();
         
         message.getBuffer().writeBytes(baos.toByteArray());
      }
      catch (IOException e)
      {
         log.error("Failed to serialise object", e);
      }
   }

   // lazy deserialize the Object the first time the client requests it
   public Serializable getObject() throws JMSException
   {
      return object;
   }

   public void clearBody() throws JMSException
   {
      super.clearBody();

      object = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

//   private static class BufferOutputStream extends OutputStream
//   {
//      private HornetQBuffer buffer;
//
//      BufferOutputStream(final HornetQBuffer buffer)
//      {
//         this.buffer = buffer;
//      }
//
//      @Override
//      public void write(final int b) throws IOException
//      {
//         buffer.writeByte((byte)b);
//      }
//   }
//
//   private static class BufferInputStream extends InputStream
//   {
//      private HornetQBuffer buffer;
//
//      BufferInputStream(final HornetQBuffer buffer)
//      {
//         this.buffer = buffer;
//      }
//
//      @Override
//      public int read() throws IOException
//      {
//         return buffer.readByte();
//      }
//   }
}
