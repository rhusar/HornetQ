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

package org.hornetq.core.persistence.impl.journal;


import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.utils.Base64;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author <a href="mailto:jbertram@redhat.com">Justin Bertram</a>
 */
public class XmlDataReader
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final Logger log = Logger.getLogger(XmlDataReader.class);

   XMLStreamReader reader;

   ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public XmlDataReader(String inputFile, String host, String port)
   {
      try
      {
         reader = XMLInputFactory.newInstance().createXMLStreamReader(new java.io.FileInputStream(inputFile));
         HashMap<String, Object> connectionParams = new HashMap<String, Object>();
         connectionParams.put(TransportConstants.HOST_PROP_NAME, host);
         connectionParams.put(TransportConstants.PORT_PROP_NAME, port);
         ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionParams));
         ClientSessionFactory sf = serverLocator.createSessionFactory();
         session = sf.createSession(false, true, true);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   // Public --------------------------------------------------------

   public static void main(String arg[])
   {
      if (arg.length < 3)
      {
         System.out.println("Use: java -cp hornetq-core.jar <inputFile> <host> <port>");
         System.exit(-1);
      }

      try
      {
         XmlDataReader xmlDataReader = new XmlDataReader(arg[0], arg[1], arg[2]);
         xmlDataReader.readXMLData();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private void readXMLData() throws Exception
   {
      while (reader.hasNext())
      {
         log.debug("EVENT:[" + reader.getLocation().getLineNumber() + "][" + reader.getLocation().getColumnNumber() + "] ");
         if (reader.getEventType() == XMLStreamConstants.START_ELEMENT)
         {
            if (XmlDataConstants.BINDINGS_CHILD.equals(reader.getLocalName()))
            {
               bindQueue();
            }
            if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName()))
            {
               sendMessage();
            }
         }
         reader.next();
      }

      session.close();
   }

   private void sendMessage() throws Exception
   {
      byte type = 0;
      boolean isLarge = false;
      byte priority = 0;
      long expiration = 0;
      long timestamp = 0;
//      HashMap<String, Object> properties = new HashMap<String, Object>();
      ArrayList<String> queues = new ArrayList<String>();

      for (int i = 0; i < reader.getAttributeCount(); i++)
      {
         String attributeName = reader.getAttributeLocalName(i);
         if (XmlDataConstants.MESSAGE_TYPE.equals(attributeName))
         {
            String value = reader.getAttributeValue(i);
            if (value.equals(XmlDataConstants.DEFAULT_TYPE_PRETTY))
            {
               type = Message.DEFAULT_TYPE;
            } else if (value.equals(XmlDataConstants.BYTES_TYPE_PRETTY))
            {
               type = Message.BYTES_TYPE;
            } else if (value.equals(XmlDataConstants.MAP_TYPE_PRETTY))
            {
               type = Message.MAP_TYPE;
            } else if (value.equals(XmlDataConstants.OBJECT_TYPE_PRETTY))
            {
               type = Message.OBJECT_TYPE;
            } else if (value.equals(XmlDataConstants.STREAM_TYPE_PRETTY))
            {
               type = Message.STREAM_TYPE;
            } else if (value.equals(XmlDataConstants.TEXT_TYPE_PRETTY))
            {
               type = Message.TEXT_TYPE;
            }
         }
         else if (XmlDataConstants.MESSAGE_IS_LARGE.equals(attributeName))
         {
            isLarge = Boolean.parseBoolean(reader.getAttributeValue(i));
         }
         else if (XmlDataConstants.MESSAGE_PRIORITY.equals(attributeName))
         {
            priority = Byte.parseByte(reader.getAttributeValue(i));
         }
         else if (XmlDataConstants.MESSAGE_EXPIRATION.equals(attributeName))
         {
            expiration = Long.parseLong(reader.getAttributeValue(i));
         }
         else if (XmlDataConstants.MESSAGE_TIMESTAMP.equals(attributeName))
         {
            timestamp = Long.parseLong(reader.getAttributeValue(i));
         }
      }

      Message message = session.createMessage(type, true, expiration, timestamp, priority);

      boolean endLoop = false;
//      StringBuilder propertiesString = new StringBuilder();
//      StringBuilder queuesString = new StringBuilder();

      while (reader.hasNext())
      {
         switch (reader.getEventType())
         {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.MESSAGE_BODY.equals(reader.getLocalName()))
               {
                  reader.next();
                  int start = reader.getTextStart();
                  int length = reader.getTextLength();
                  message.getBodyBuffer().writeBytes(decode(new String(reader.getTextCharacters(), start, length).trim()));
               }
               else if (XmlDataConstants.PROPERTIES_CHILD.equals(reader.getLocalName()))
               {
                  String key = "";
                  String value = "";
                  String propertyType = "";

                  for (int i = 0; i < reader.getAttributeCount(); i++)
                  {
                     String attributeName = reader.getAttributeLocalName(i);
                     if (XmlDataConstants.PROPERTY_NAME.equals(attributeName))
                     {
//                        propertiesString.append("(" + XmlDataConstants.PROPERTY_NAME + "=" + reader.getAttributeValue(i));
                        key = reader.getAttributeValue(i);
                     }
                     else if (XmlDataConstants.PROPERTY_VALUE.equals(attributeName))
                     {
//                        propertiesString.append("," + XmlDataConstants.PROPERTY_VALUE + "=" + reader.getAttributeValue(i));
                        value = reader.getAttributeValue(i);
                     }
                     else if (XmlDataConstants.PROPERTY_TYPE.equals(attributeName))
                     {
//                        propertiesString.append(", " + XmlDataConstants.PROPERTY_TYPE + "=" + reader.getAttributeValue(i) + ") ");
                        propertyType = reader.getAttributeValue(i);
                     }
                  }

                  if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_SHORT))
                  {
                     message.putShortProperty(key, Short.parseShort(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_BOOLEAN))
                  {
                     message.putBooleanProperty(key, Boolean.parseBoolean(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_BYTE))
                  {
                     message.putByteProperty(key, Byte.parseByte(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_DOUBLE))
                  {
                     message.putDoubleProperty(key, Double.parseDouble(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_FLOAT))
                  {
                     message.putFloatProperty(key, Float.parseFloat(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_INTEGER))
                  {
                     message.putIntProperty(key, Integer.parseInt(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_LONG))
                  {
                     message.putLongProperty(key, Long.parseLong(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_SIMPLE_STRING))
                  {
                     message.putStringProperty(new SimpleString(key), new SimpleString(value));
                  }
                  else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_STRING))
                  {
                     message.putStringProperty(key, value);
                  }
               }
               else if (XmlDataConstants.QUEUES_CHILD.equals(reader.getLocalName()))
               {
                  for (int i = 0; i < reader.getAttributeCount(); i++)
                  {
                     String attributeName = reader.getAttributeLocalName(i);
                     if (XmlDataConstants.QUEUE_NAME.equals(attributeName))
                     {
//                        queuesString.append("(" + XmlDataConstants.QUEUE_NAME + "=" + reader.getAttributeValue(i) + ")");
                        queues.add(reader.getAttributeValue(i));
                     }
                  }
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName()))
               {
                  endLoop = true;
               }
               break;
         }
         if (endLoop)
         {
            break;
         }
         reader.next();
      }

      for(String queueName : queues)
      {
         System.out.println("To " + queueName + ": " + message);
         ClientProducer producer = session.createProducer(queueName);
         producer.send(message);
      }

//      System.out.println("Sending message (type=" + type + ", isLarge=" + isLarge + ", priority=" + priority + ", expiration=" + expiration + ", timestamp=" + timestamp + ", properties=" + properties + ", queues=" + queues + ", body=" + new String(decode(body.trim())) + ")");

   }

   private void bindQueue() throws Exception
   {
      String queueName = "";
      String address = "";
      String filter = "";

      for (int i = 0; i < reader.getAttributeCount(); i++)
      {
         String attributeName = reader.getAttributeLocalName(i);
         if (XmlDataConstants.BINDING_ADDRESS.equals(attributeName))
         {
            address = reader.getAttributeValue(i);
         }
         else if (XmlDataConstants.BINDING_QUEUE_NAME.equals(attributeName))
         {
            queueName = reader.getAttributeValue(i);
         }
         else if (XmlDataConstants.BINDING_FILTER_STRING.equals(attributeName))
         {
            filter = reader.getAttributeValue(i);
         }
      }

      session.createQueue(address, queueName, filter, true);

      System.out.println("Binding queue(name=" + queueName + ", address=" + address + ", filter=" + filter + ")");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static byte[] decode(String data)
   {
      return Base64.decode(data);
   }

   // Inner classes -------------------------------------------------

}