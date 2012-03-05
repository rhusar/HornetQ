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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.utils.Base64;
import org.hornetq.utils.ExecutorFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hornetq.core.persistence.impl.journal.JournalStorageManager.PersistentQueueBindingEncoding;
import static org.hornetq.core.persistence.impl.journal.JournalStorageManager.ReferenceDescribe;
import static org.hornetq.core.persistence.impl.journal.JournalStorageManager.MessageDescribe;
import static org.hornetq.core.persistence.impl.journal.JournalStorageManager.CursorAckRecordEncoding;
import static org.hornetq.core.persistence.impl.journal.JournalStorageManager.PageUpdateTXEncoding;
import static org.hornetq.core.persistence.impl.journal.JournalStorageManager.AckDescribe;

/**
 * @author <a href="mailto:jbertram@redhat.com">Justin Bertram</a>
 */
public class XMLDataWriter
{
   public static final String XML_VERSION = "1.0";
   public static final String DOCUMENT_PARENT = "hornetq-journal";
   public static final String BINDINGS_PARENT = "bindings";
   public static final String BINDINGS_CHILD = "binding";
   public static final String BINDING_ADDRESS = "address";
   public static final String BINDING_FILTER_STRING = "filter-string";
   public static final String BINDING_QUEUE_NAME = "queue-name";
   public static final String BINDING_ID = "id";
   public static final String MESSAGES_PARENT = "messages";
   public static final String MESSAGES_CHILD = "message";
   public static final String MESSAGE_ID = "id";
   public static final String MESSAGE_PRIORITY = "priority";
   public static final String MESSAGE_EXPIRATION = "expiration";
   public static final String MESSAGE_TIMESTAMP = "timestamp";
   public static final String DEFAULT_TYPE_PRETTY = "default";
   public static final String BYTES_TYPE_PRETTY = "bytes";
   public static final String MAP_TYPE_PRETTY = "map";
   public static final String OBJECT_TYPE_PRETTY = "object";
   public static final String STREAM_TYPE_PRETTY = "stream";
   public static final String TEXT_TYPE_PRETTY = "text";
   public static final String MESSAGE_TYPE = "type";
   public static final String MESSAGE_USER_ID = "user-id";
   public static final String MESSAGE_BODY = "body";
   public static final String PROPERTIES_PARENT = "properties";
   public static final String PROPERTIES_CHILD = "property";
   public static final String PROPERTY_NAME = "name";
   public static final String PROPERTY_VALUE = "value";
   public static final String PROPERTY_TYPE = "type";
   public static final String QUEUES_PARENT = "queues";
   public static final String QUEUES_CHILD = "queue";
   public static final String QUEUE_NAME = "name";
   public static final String PROPERTY_TYPE_BOOLEAN = "boolean";
   public static final String PROPERTY_TYPE_BYTE = "byte";
   public static final String PROPERTY_TYPE_SHORT = "short";
   public static final String PROPERTY_TYPE_INTEGER = "integer";
   public static final String PROPERTY_TYPE_LONG = "long";
   public static final String PROPERTY_TYPE_FLOAT = "float";
   public static final String PROPERTY_TYPE_DOUBLE = "double";
   public static final String PROPERTY_TYPE_STRING = "string";
   public static final String PROPERTY_TYPE_SIMPLE_STRING = "simple-string";

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private JournalStorageManager storageManager;

   private Configuration config;

   private XMLStreamWriter xmlWriter;

   // an inner map of message refs hashed by the queue ID to which they belong and then hashed by their record ID
   private Map<Long, HashMap<Long, ReferenceDescribe>> messageRefs;

   // map of all message records hashed by their record ID (which will match the record ID of the message refs)
   private HashMap<Long, Message> messages;

   private Map<Long, Set<PagePosition>> cursorRecords;

   private Set<Long> pgTXs;

   HashMap<Long, PersistentQueueBindingEncoding> queueBindings;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public XMLDataWriter(String bindingsDir, String journalDir, String pagingDir, String largeMessagesDir)
   {
      config = new ConfigurationImpl();
      config.setBindingsDirectory(bindingsDir);
      config.setJournalDirectory(journalDir);
      config.setPagingDirectory(pagingDir);
      config.setLargeMessagesDirectory(largeMessagesDir);
      config.setJournalType(JournalType.NIO);
      final ExecutorService executor = Executors.newFixedThreadPool(1);
      ExecutorFactory executorFactory = new ExecutorFactory()
      {
         public Executor getExecutor()
         {
            return executor;
         }
      };
      
      storageManager = new JournalStorageManager(config, executorFactory);

      messageRefs = new HashMap<Long, HashMap<Long, ReferenceDescribe>>();

      messages = new HashMap<Long, Message>();

      cursorRecords = new HashMap<Long, Set<PagePosition>>();

      pgTXs = new HashSet<Long>();

      queueBindings = new HashMap<Long, PersistentQueueBindingEncoding>();

      try
      {
         XMLOutputFactory factory = XMLOutputFactory.newInstance();
         XMLStreamWriter rawXmlWriter = factory.createXMLStreamWriter(System.out);
         PrettyPrintHandler handler = new PrettyPrintHandler(rawXmlWriter);
         xmlWriter = (XMLStreamWriter) Proxy.newProxyInstance(
               XMLStreamWriter.class.getClassLoader(),
               new Class[]{XMLStreamWriter.class},
               handler);
      } catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   // Public --------------------------------------------------------

   public static void main(String arg[])
   {
      if (arg.length < 4)
      {
         System.out.println("Use: java -cp hornetq-core.jar <bindings directory> <message directory> <page directory> <large-message directory>");
         System.exit(-1);
      }

      try
      {         
         XMLDataWriter xmlDataWriter = new XMLDataWriter(arg[0], arg[1], arg[2], arg[3]);
         xmlDataWriter.getBindings();
         xmlDataWriter.processMessageJournal();
         xmlDataWriter.printDataAsXML();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void processMessageJournal() throws Exception
   {
      ArrayList<RecordInfo> acks = new ArrayList<RecordInfo>();

      List<RecordInfo> records = new LinkedList<RecordInfo>();

      Journal messageJournal = storageManager.getMessageJournal();

      messageJournal.start();

      ((JournalImpl)messageJournal).load(records, null, null, false);

      for (RecordInfo info : records)
      {
         byte[] data = info.data;

         HornetQBuffer buff = HornetQBuffers.wrappedBuffer(data);

         Object o = JournalStorageManager.newObjectEncoding(info, storageManager);
         if (info.getUserRecordType() == JournalStorageManager.ADD_MESSAGE)
         {
            messages.put(info.id, ((MessageDescribe) o).msg);
         }
         else if (info.getUserRecordType() == JournalStorageManager.ADD_LARGE_MESSAGE)
         {
            messages.put(info.id, ((MessageDescribe) o).msg);
         }
         else if (info.getUserRecordType() == JournalStorageManager.ADD_REF)
         {
            ReferenceDescribe ref = (ReferenceDescribe) o;
            HashMap<Long, ReferenceDescribe> map = messageRefs.get(info.id);
            if (map == null)
            {
               HashMap<Long, ReferenceDescribe> newMap = new HashMap<Long, ReferenceDescribe>();
               newMap.put(ref.refEncoding.queueID, ref);
               messageRefs.put(info.id, newMap);
            }
            else
            {
               map.put(ref.refEncoding.queueID, ref);
            }
         }
         else if (info.getUserRecordType() == JournalStorageManager.ACKNOWLEDGE_REF)
         {
            acks.add(info);
         }
         else if (info.userRecordType == JournalStorageManager.ACKNOWLEDGE_CURSOR)
         {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
            encoding.decode(buff);

            Set<PagePosition> set = cursorRecords.get(encoding.queueID);

            if (set == null)
            {
               set = new HashSet<PagePosition>();
               cursorRecords.put(encoding.queueID, set);
            }

            set.add(encoding.position);
         }
         else if (info.userRecordType == JournalStorageManager.PAGE_TRANSACTION)
         {
            if (info.isUpdate)
            {
               PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

               pageUpdate.decode(buff);
               pgTXs.add(pageUpdate.pageTX);
            }
            else
            {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buff);

               pageTransactionInfo.setRecordID(info.id);
               pgTXs.add(pageTransactionInfo.getTransactionID());
            }
         }
      }

      messageJournal.stop();

      removeAcked(acks);
   }

   private void removeAcked(ArrayList<RecordInfo> acks)
   {
      for (RecordInfo info : acks)
      {
         AckDescribe ack = (AckDescribe) JournalStorageManager.newObjectEncoding(info, null);
         HashMap<Long, ReferenceDescribe> referenceDescribeHashMap = messageRefs.get(info.id);
         referenceDescribeHashMap.remove(ack.refEncoding.queueID);
         if (referenceDescribeHashMap.size() == 0)
         {
            messages.remove(info.id);
            messageRefs.remove(info.id);
         }
      }
   }

   private void getBindings() throws Exception
   {
      List<RecordInfo> records = new LinkedList<RecordInfo>();

      Journal bindingsJournal = storageManager.getBindingsJournal();

      bindingsJournal.start();

      ((JournalImpl)bindingsJournal).load(records, null, null, false);

      for (RecordInfo info : records)
      {
         if (info.getUserRecordType() == JournalStorageManager.QUEUE_BINDING_RECORD)
         {
            PersistentQueueBindingEncoding bindingEncoding = (PersistentQueueBindingEncoding) JournalStorageManager.newObjectEncoding(info, null);
            queueBindings.put(bindingEncoding.getId(), bindingEncoding);
         }
      }

      bindingsJournal.stop();
   }

   private void printDataAsXML()
   {
      try
      {
         xmlWriter.writeStartDocument(XML_VERSION);
         xmlWriter.writeStartElement(DOCUMENT_PARENT);
         printBindingsAsXML();
         printAllMessagesAsXML();
         xmlWriter.writeEndElement(); // end DOCUMENT_PARENT
         xmlWriter.writeEndDocument();
         xmlWriter.flush();
         xmlWriter.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private void printBindingsAsXML() throws XMLStreamException
   {
      xmlWriter.writeStartElement(BINDINGS_PARENT);
      for (Map.Entry<Long, PersistentQueueBindingEncoding> queueBindingEncodingEntry : queueBindings.entrySet())
      {
         PersistentQueueBindingEncoding bindingEncoding = queueBindings.get(queueBindingEncodingEntry.getKey());
         xmlWriter.writeEmptyElement(BINDINGS_CHILD);
         xmlWriter.writeAttribute(BINDING_ADDRESS, bindingEncoding.getAddress().toString());
         String filter = "";
         if (bindingEncoding.getFilterString() != null)
         {
            filter = bindingEncoding.getFilterString().toString();
         }
         xmlWriter.writeAttribute(BINDING_FILTER_STRING, filter);
         xmlWriter.writeAttribute(BINDING_QUEUE_NAME, bindingEncoding.getQueueName().toString());
         xmlWriter.writeAttribute(BINDING_ID, Long.toString(bindingEncoding.getId()));
      }
      xmlWriter.writeEndElement(); // end BINDINGS_PARENT
   }

   private void printAllMessagesAsXML() throws XMLStreamException
   {
      xmlWriter.writeStartElement(MESSAGES_PARENT);

      for (Map.Entry<Long, Message> messageMapEntry : messages.entrySet())
      {
         printSingleMessageAsXML((ServerMessage) messageMapEntry.getValue(), extractQueueNames(messageRefs.get(messageMapEntry.getKey())));
      }

      printPagedMessagesAsXML();

      xmlWriter.writeEndElement(); // end "messages"
   }

   private void printPagedMessagesAsXML()
   {
      try
      {
         ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
         final ExecutorService executor = Executors.newFixedThreadPool(10);
         ExecutorFactory execfactory = new ExecutorFactory()
         {
            public Executor getExecutor()
            {
               return executor;
            }
         };
         PagingStoreFactory pageStoreFactory = new PagingStoreFactoryNIO(config.getPagingDirectory(), 1000l, scheduled, execfactory, false, null);
         HierarchicalRepository<AddressSettings> addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>();
         addressSettingsRepository.setDefault(new AddressSettings());
         StorageManager sm = new NullStorageManager();
         PagingManager manager = new PagingManagerImpl(pageStoreFactory, sm, addressSettingsRepository);

         manager.start();

         SimpleString stores[] = manager.getStoreNames();

         for (SimpleString store : stores)
         {
            PagingStore pageStore = manager.getPageStore(store);

            int pageId = (int) pageStore.getFirstPage();
            for (int i = 0; i < pageStore.getNumberOfPages(); i++)
            {
               Page page = pageStore.createPage(pageId);
               page.open();
               List<PagedMessage> messages = page.read(sm);
               page.close();

               int messageId = 0;

               for (PagedMessage message : messages)
               {
                  message.initMessage(sm);
                  long queueIDs[] = message.getQueueIDs();
                  List<String> queueNames = new ArrayList<String>();
                  for (long queueID : queueIDs)
                  {
                     PagePosition posCheck = new PagePositionImpl(pageId, messageId);

                     boolean acked = false;

                     Set<PagePosition> positions = cursorRecords.get(queueID);
                     if (positions != null)
                     {
                        acked = positions.contains(posCheck);
                     }

                     if (!acked)
                     {
                        queueNames.add(queueBindings.get(queueID).getQueueName().toString());
                     }
                  }

                  if (queueNames.size() > 0 && (message.getTransactionID() == -1 || pgTXs.contains(message.getTransactionID())))
                  {
                     printSingleMessageAsXML(message.getMessage(), queueNames);
                  }

                  messageId++;
               }

               pageId++;
            }
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private void printSingleMessageAsXML(ServerMessage message, List<String> queues) throws XMLStreamException
   {
      xmlWriter.writeStartElement(MESSAGES_CHILD);
      xmlWriter.writeAttribute(MESSAGE_ID, Long.toString(message.getMessageID()));
      xmlWriter.writeAttribute(MESSAGE_PRIORITY, Byte.toString(message.getPriority()));
      xmlWriter.writeAttribute(MESSAGE_EXPIRATION, Long.toString(message.getExpiration()));
      xmlWriter.writeAttribute(MESSAGE_TIMESTAMP, Long.toString(message.getTimestamp()));
      byte rawType = message.getType();
      String prettyType = DEFAULT_TYPE_PRETTY;
      if (rawType == Message.BYTES_TYPE)
      {
         prettyType = BYTES_TYPE_PRETTY;
      }
      else if (rawType == Message.MAP_TYPE)
      {
         prettyType = MAP_TYPE_PRETTY;
      }
      else if (rawType == Message.OBJECT_TYPE)
      {
         prettyType = OBJECT_TYPE_PRETTY;
      }
      else if (rawType == Message.STREAM_TYPE)
      {
         prettyType = STREAM_TYPE_PRETTY;
      }
      else if (rawType == Message.TEXT_TYPE)
      {
         prettyType = TEXT_TYPE_PRETTY;
      }
      xmlWriter.writeAttribute(MESSAGE_TYPE, prettyType);
      xmlWriter.writeAttribute(MESSAGE_USER_ID, message.getUserID().toString());
      xmlWriter.writeStartElement(MESSAGE_BODY);
      if (message.isLargeMessage())
      {
         LargeServerMessage largeMessage = (LargeServerMessage) message;
         final int CHUNK = 1000;

         try
         {
            BodyEncoder encoder = largeMessage.getBodyEncoder();
            encoder.open();
            for (long i = 0; i < encoder.getLargeBodySize(); i += CHUNK)
            {
               HornetQBuffer buffer = HornetQBuffers.fixedBuffer(CHUNK);
               encoder.encode(buffer, CHUNK);
               xmlWriter.writeCharacters(encode(buffer.toByteBuffer().array()));
            }
         }
         catch (HornetQException e)
         {
            e.printStackTrace();
         }
      }
      else
      {
         xmlWriter.writeCharacters(encode(message.getBodyBuffer().toByteBuffer().array()));
      }
      xmlWriter.writeEndElement();
      xmlWriter.writeStartElement(PROPERTIES_PARENT);
      for (SimpleString key : message.getPropertyNames())
      {
         Object value = message.getObjectProperty(key);
         xmlWriter.writeEmptyElement(PROPERTIES_CHILD);
         xmlWriter.writeAttribute(PROPERTY_NAME, key.toString());
         xmlWriter.writeAttribute(PROPERTY_VALUE, value.toString());
         if (value instanceof Boolean)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_BOOLEAN);
         }
         else if (value instanceof Byte)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_BYTE);
         }
         else if (value instanceof Short)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_SHORT);
         }
         else if (value instanceof Integer)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_INTEGER);
         }
         else if (value instanceof Long)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_LONG);
         }
         else if (value instanceof Float)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_FLOAT);
         }
         else if (value instanceof Double)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_DOUBLE);
         }
         else if (value instanceof String)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_STRING);
         }
         else if (value instanceof SimpleString)
         {
            xmlWriter.writeAttribute(PROPERTY_TYPE, PROPERTY_TYPE_SIMPLE_STRING);
         }
      }
      xmlWriter.writeEndElement(); // end "properties"
      xmlWriter.writeStartElement(QUEUES_PARENT);
      for (String queueName : queues)
      {
         xmlWriter.writeEmptyElement(QUEUES_CHILD);
         xmlWriter.writeAttribute(QUEUE_NAME, queueName);
      }
      xmlWriter.writeEndElement(); // end "queues"
      xmlWriter.writeEndElement(); // end "message"
   }

   private static String encode(final byte[] data)
   {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   private List<String> extractQueueNames(HashMap<Long, ReferenceDescribe> refMap)
   {
      List<String> queues = new ArrayList<String>();
      for (ReferenceDescribe ref : refMap.values())
      {
         queues.add(queueBindings.get(ref.refEncoding.queueID).getQueueName().toString());
      }
      return queues;
   }

   // Inner classes -------------------------------------------------

   class PrettyPrintHandler implements InvocationHandler
   {
      private XMLStreamWriter target;

      private int depth = 0;

      private static final char INDENT_CHAR = ' ';

      private static final String LINEFEED_CHAR = "\n";


      public PrettyPrintHandler(XMLStreamWriter target)
      {
         this.target = target;
      }

      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
      {
         String m = method.getName();

         if ("writeStartElement".equals(m))
         {
            target.writeCharacters(LINEFEED_CHAR);
            target.writeCharacters(indent(depth, INDENT_CHAR));

            depth++;
         }
         else if ("writeEndElement".equals(m))
         {
            depth--;

            target.writeCharacters(LINEFEED_CHAR);
            target.writeCharacters(indent(depth, INDENT_CHAR));
         }
         else if ("writeEmptyElement".equals(m) || "writeCharacters".equals(m))
         {
            target.writeCharacters(LINEFEED_CHAR);
            target.writeCharacters(indent(depth, INDENT_CHAR));
         }

         method.invoke(target, args);

         return null;
      }

      private String indent(int d, char s)
      {
         d *= 3; // level of indentation
         char[] output = new char[d];
         while (d-- > 0)
         {
            output[d] = INDENT_CHAR;
         }
         return new String(output);
      }
   }
}