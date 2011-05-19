/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.persistence.tools;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.tools.xmlmodel.BindingsJournalType;
import org.hornetq.core.persistence.tools.xmlmodel.HornetQExport;
import org.hornetq.core.persistence.tools.xmlmodel.MessageType;
import org.hornetq.core.persistence.tools.xmlmodel.MessagesExportType;
import org.hornetq.core.persistence.tools.xmlmodel.PropertyType;
import org.hornetq.core.persistence.tools.xmlmodel.QueueRefType;
import org.hornetq.core.persistence.tools.xmlmodel.QueueType;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.Base64;
import org.hornetq.utils.ExecutorFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * @author <a href="mailto:torben@redhat.com">Torben Jaeger</a>
 */
public class ManageDataTool extends JournalStorageManager
{
   private static final Logger log = Logger.getLogger(ManageDataTool.class);

   private static long messageCounter = 0;

   private ManageDataTool(final Configuration config, final ExecutorFactory executorFactory)
   {
      super(config, executorFactory);
   }

   /**
    * Method which will export the binary HQ journal to a XML representation of messages and bindings.
    * <p/>
    * Some rules to keep in mind:
    * <ul>
    * <li> not all records of the journals are exported (no 1-to-1 copy); see exporting rules below.
    * <li> the binary journal should not be started/loaded
    * </ul>
    * <p/>
    * Rules for exporting:
    * <ul>
    * <li> ADD records are exported
    * <li> REF records are exported
    * <li> records with ACKs are <b>not</b> exported coz the messages have already been delivered
    * <li> records which are part of a prepared TX are <b>not</b> exported due to recovery
    * <li> records which are part of failed TX are <b>not</b> exported due to recovery
    * <li> records with a delete request are <b>not</b> exported coz the messages have already been delivered
    * <li> records with internal message headers (like routing) are <b>not</b> exported
    * </ul>
    * <p/>
    *
    * @param bindingsDir directory with the bindings journal
    * @param messagesDir directory with the messages journal
    * @throws Exception if something goes wrong
    */
   public static void exportMessages(final String bindingsDir, final String messagesDir, final OutputStream out) throws Exception
   {

      // Will use only default values. The load function should adapt to anything different
      ConfigurationImpl defaultValues = new ConfigurationImpl();
      defaultValues.setJournalDirectory(messagesDir);

      if (log.isInfoEnabled())
      {
         log.info("Generating backup of original journal ...");
      }

      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(messagesDir);
      JournalImpl messagesJournal = new JournalImpl(defaultValues.getJournalFileSize(),
                                                    defaultValues.getJournalMinFiles(),
                                                    0,
                                                    0,
                                                    messagesFF,
                                                    "hornetq-data",
                                                    "hq",
                                                    1);

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir);
      JournalImpl bindingsJournal = new JournalImpl(1024 * 1024,
                                                    2,
                                                    -1,
                                                    0,
                                                    bindingsFF,
                                                    "hornetq-bindings",
                                                    "bindings",
                                                    1);

      HornetQExport hqJournalExport = new HornetQExport();

      if (log.isInfoEnabled())
      {
         log.info("Exporting bindings ...");
      }
      hqJournalExport.setQueues(exportBindings(bindingsJournal));

      if (log.isInfoEnabled())
      {
         log.info("Exporting journal ...");
      }
      hqJournalExport.setMessages(exportMessages(messagesJournal));

      if (log.isInfoEnabled())
      {
         log.info("Writing export file ...");
      }
      writeExportToFile(hqJournalExport, out);

      if (log.isInfoEnabled())
      {
         log.info("Export done!");
      }
   }

   private static BindingsJournalType exportBindings(JournalImpl original) throws Exception
   {
      try
      {
         final List<RecordInfo> records = new LinkedList<RecordInfo>();
         final Set<Long> recordsToDelete = new HashSet<Long>();

         loadData(original, records, recordsToDelete);

         return buildXmlBindings(records);

      }
      finally
      {
         original.stop();
      }
   }

   /**
    * Method to load the journal and prepare the record lists/sets to work on.
    *
    * @param original the journal to export
    * @return an XML representation of the journal
    * @throws Exception if the journal is corrupt
    */
   private static MessagesExportType exportMessages(JournalImpl original) throws Exception
   {

      try
      {
         final List<RecordInfo> records = new LinkedList<RecordInfo>();
         final Set<Long> recordsToDelete = new HashSet<Long>();

         loadData(original, records, recordsToDelete);

         MessagesExportType journal = buildXmlMessages(records, recordsToDelete);

         // addDelRecordsToOriginal(original, recordsToDelete, journal.getMessage());

         return journal;

      }
      finally
      {
         original.stop();
      }

   }

   private static void loadData(Journal original, final List<RecordInfo> records, final Set<Long> recordsToDelete) throws Exception
   {
      original.start();

      original.load(new LoaderCallback()
      {
         private void logNotExportedRecord(long id, String reason)
         {
            if (log.isDebugEnabled())
            {
               log.debug("Record " + id + " will not be exported due to " + reason + "!");
            }
         }

         private void addToNotExportedRecords(List<RecordInfo> list, String reason)
         {
            for (RecordInfo record : list)
            {
               logNotExportedRecord(record.id, reason);
               recordsToDelete.add(record.id);
            }
         }

         public void addPreparedTransaction(PreparedTransactionInfo preparedTransaction)
         {
            addToNotExportedRecords(preparedTransaction.records, "prepared TX");
            addToNotExportedRecords(preparedTransaction.recordsToDelete, "prepared TX");
         }

         public void addRecord(RecordInfo info)
         {
            records.add(info);
         }

         public void deleteRecord(long id)
         {
            logNotExportedRecord(id, "DEL");
            recordsToDelete.add(id);
         }

         public void updateRecord(RecordInfo info)
         {
            records.add(info);
         }

         public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> _recordsToDelete)
         {
            addToNotExportedRecords(records, "failed TX");
            addToNotExportedRecords(_recordsToDelete, "failed TX");
         }

      });
   }

   /**
    * Method which writes the XML export file to disk.
    *
    * @param hqJournalExport the root JAXB context
    * @throws java.io.FileNotFoundException if the export file could not be created
    * @throws javax.xml.bind.JAXBException if an error occurs during marshalling
    */
   private static void writeExportToFile(HornetQExport hqJournalExport, OutputStream os) throws JAXBException,
                                                                                        FileNotFoundException
   {

      // todo: http://jaxb.java.net/guide/Different_ways_of_marshalling.html#Marshalling_into_a_subtree

      JAXBContext jc = JAXBContext.newInstance(HornetQExport.class);
      Marshaller m = jc.createMarshaller();
      m.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      m.marshal(hqJournalExport, os);
   }

   /**
    * Method which creates the JAXB representation of the XML export file.
    *
    * @param records         the records to process
    * @param recordsToDelete records which are @see DeleteRecord
    * @return @see BindingsJournalType or @see MessagesJournalType
    */
   private static MessagesExportType buildXmlMessages(List<RecordInfo> records, Set<Long> recordsToDelete)
   {

      MessagesExportType journalType = new MessagesExportType();

      // Export Journal
      for (RecordInfo info : records)
      {

         if (recordsToDelete.contains(info.id))
         {
            // deleted records are not exported
            continue;
         }

         switch (info.getUserRecordType())
         {

            case JournalStorageManager.ADD_MESSAGE:
               handleAddMessage(journalType, info);
               break;

            case JournalStorageManager.ADD_REF:
               handleAddRef(journalType, info);
               break;

            case JournalStorageManager.ACKNOWLEDGE_REF:
               handleAckRef(journalType, info);
               break;

            default:
               if (log.isDebugEnabled())
               {
                  log.debug(new StringBuilder().append("Record ")
                                               .append(info.id)
                                               .append(" is not exported!")
                                               .toString());
               }
               break;
         }
      }

      return journalType;
   }

   private static void handleAddMessage(MessagesExportType journalType, RecordInfo info)
   {
      JournalStorageManager.MessageDescribe message = (JournalStorageManager.MessageDescribe)JournalStorageManager.newObjectEncoding(info);

      MessageType messageType = new MessageType((ServerMessage)message.msg);

      byte[] data = info.data;
      messageType.setPayload(Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE));

      journalType.getMessage().add(messageType);
   }

   private static void handleAddRef(MessagesExportType journalType, RecordInfo info)
   {
      JournalStorageManager.ReferenceDescribe ref = (JournalStorageManager.ReferenceDescribe)JournalStorageManager.newObjectEncoding(info);

      MessageType message = getMessage(journalType, info);

      if (message == null)
      {
         throw new IllegalStateException("Journal is corrupt: AddRef without Add!");
      }

      QueueRefType queueRef = new QueueRefType();
      queueRef.setId(ref.refEncoding.queueID);

      message.getBindings().getQueue().add(queueRef);

   }

   private static MessageType getMessage(MessagesExportType journalType, RecordInfo info)
   {
      List<MessageType> messages = journalType.getMessage();
      return messages.get(messages.indexOf(new MessageType(info.id)));
   }

   private static void handleAckRef(MessagesExportType journalType, RecordInfo info)
   {
      JournalStorageManager.AckDescribe ack = (JournalStorageManager.AckDescribe)JournalStorageManager.newObjectEncoding(info);

      MessageType message = getMessage(journalType, info);

      if (message == null)
      {
         throw new IllegalStateException("Journal is corrupt: Ack without Add!");
      }

      QueueRefType queueRef = new QueueRefType();
      queueRef.setId(ack.refEncoding.queueID);

      message.getAcks().getQueue().add(queueRef);

   }

   private static BindingsJournalType buildXmlBindings(List<RecordInfo> records)
   {

      BindingsJournalType journalType = new BindingsJournalType();

      // Export Journal
      for (RecordInfo info : records)
      {

         switch (info.getUserRecordType())
         {

            case JournalStorageManager.QUEUE_BINDING_RECORD:
               handleQBindingRecord(journalType, info);
               break;

            default:
               if (log.isDebugEnabled())
               {
                  log.debug(new StringBuilder().append("Record ")
                                               .append(info.id)
                                               .append(" is not exported!")
                                               .toString());
               }
               break;

         }

      }

      return journalType;
   }

   private static void handleQBindingRecord(BindingsJournalType journalType, RecordInfo info)
   {
      JournalStorageManager.PersistentQueueBindingEncoding persistentQueueBindingEncoding = JournalStorageManager.newBindingEncoding(info.id,
                                                                                                                                     HornetQBuffers.wrappedBuffer(info.data));

      QueueType queueType = new QueueType();
      queueType.setId(persistentQueueBindingEncoding.getId());

      SimpleString queueName = persistentQueueBindingEncoding.getQueueName();
      queueType.setName(queueName != null ? queueName.toString() : "");
      SimpleString address = persistentQueueBindingEncoding.getAddress();
      queueType.setAddress(address != null ? address.toString() : "");
      SimpleString filterString = persistentQueueBindingEncoding.getFilterString();
      queueType.setFilterString(filterString != null ? filterString.toString() : "");

      journalType.getQueue().add(queueType);
   }

   /**
    * Imports messages which were exported as a XML representation using @see #exportMessages(String,String,String).
    *
    * This is done by starting an embedded HQ server which connects the queues transparently.
    *
    * @param importFile  full qualified name of the import file (/path/file)
    * @param configurationFile the configuration URL of a running HQ server
    * @param connectorName the name of the connector used to connect to the HQ server
    * @return the number of imported records
    * @throws Exception if an error occurs while importing
    */
   public static long importMessages(final String importFile, final String configurationFile, final String connectorName) throws Exception
   {
      FileConfiguration configuration = null;
      try
      {
         configuration = new FileConfiguration();
         configuration.setConfigurationUrl(configurationFile);
         configuration.start();
      }
      finally
      {
         if (configuration != null)
         {
            configuration.stop();
         }
      }

      ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(configuration.getConnectorConfigurations()
                                                                                            .get(connectorName));

      FileInputStream input = new FileInputStream(new File(importFile));

      return importMessages(input, serverLocator);
   }

   public static long importMessages(final InputStream importFile,
                                     final ServerLocator serverLocator) throws Exception
   {
      final JAXBContext context = JAXBContext.newInstance(HornetQExport.class);

      final Unmarshaller unmarshaller = context.createUnmarshaller();

      final Map<Long, Long> queueMapping = new HashMap<Long, Long>();

      ClientSessionFactory sf = null;
      
      try
      {
         sf = serverLocator.createSessionFactory();
         final ClientSession coreSession = sf.createSession();

         // message notification callback
         final MessagesExportType.Listener listener = new MessagesExportType.Listener()
         {
            public void handleMessage(MessageType message) throws Exception {
               final List<QueueType> originalQueues = message.getAllPreviousBindings().getQueue();
               final List<QueueRefType> originalBindings = message.getBindings().getQueue();
               QueueType queue;
               ClientSession.QueueQuery queueQuery;
               for (QueueRefType originalBinding : originalBindings) {
                  queue = originalQueues.get(originalQueues.indexOf(new QueueType(originalBinding.getId())));
                  queueQuery = coreSession.queueQuery(SimpleString.toSimpleString(queue.getName()));

                  if (!queueQuery.isExists()) {
                     coreSession.createQueue(queue.getAddress(), queue.getName(), queue.getFilterString(), message.isDurable());
                     queueQuery = coreSession.queueQuery(SimpleString.toSimpleString(queue.getName()));
                     queueMapping.put(queue.getId(), queueQuery.getId());
                  }
               }

               ClientProducer producer = coreSession.createProducer(message.getAddress());
               ClientMessage clientMessage = generateClientMessage(message);
               producer.send(clientMessage);
               producer.close();
            }

            private ClientMessage generateClientMessage(MessageType message) throws IOException
            {
               ClientMessage clientMessage = coreSession.createMessage(message.getType(),
                                                                       message.isDurable(),
                                                                       message.getExpiration(),
                                                                       message.getTimestamp(),
                                                                       message.getPriority());
               List<QueueRefType> bindings = message.getBindings().getQueue();
               List<QueueRefType> ackedQueues = message.getAcks().getQueue();
               List<Long> queues = new ArrayList<Long>(bindings.size());

               // Adress
               clientMessage.setAddress(new SimpleString(message.getAddress()));

               // Routing
               // only map Q if not already ACKed
               for (QueueRefType binding : bindings)
               {
                  if (!ackedQueues.contains(new QueueRefType(binding.getId())))
                  {
                     queues.add(queueMapping.get(binding.getId()));
                  }
               }
               clientMessage.putBytesProperty(MessageImpl.HDR_ROUTE_TO_IDS, getByteArrayOf(queues));

               // Properties
               List<PropertyType> properties = message.getProperties().getProperty();
               for (PropertyType property : properties)
               {
                  clientMessage.putStringProperty(property.getKey(), property.getValue());
               }

               // Payload
               HornetQBuffer buffer = clientMessage.getBodyBuffer();
               buffer.writeBytes(Base64.decode(message.getPayload(), Base64.DONT_BREAK_LINES | Base64.URL_SAFE));

               // UserID
               // todo: need to set?

               return clientMessage;
            }

            private byte[] getByteArrayOf(List<Long> queues) throws IOException
            {
               ByteArrayOutputStream bos = new ByteArrayOutputStream();
               DataOutputStream dos = new DataOutputStream(bos);
               try
               {
                  for (long id : queues)
                  {
                     dos.writeLong(id);
                  }
                  dos.flush();
               }
               finally
               {
                  try
                  {
                     dos.close();
                     bos.close();
                  }
                  catch (IOException e)
                  {
                     // could not close streams
                  }
               }

               return bos.toByteArray();
            }

         };

         messageCounter = 0;

         // install the callback on all message instances
         unmarshaller.setListener(new Unmarshaller.Listener()
         {
            public void beforeUnmarshal(Object target, Object parent)
            {
               if (target instanceof MessagesExportType)
               {
                  ((MessagesExportType)target).setMessageListener(listener);
                  if (((MessagesExportType) target).getBindings() == null) {
                     ((MessagesExportType)target).setOriginalBindings(((HornetQExport)parent).getQueues());
                  }
               }
            }

            public void afterUnmarshal(Object target, Object parent)
            {
               if (target instanceof MessagesExportType)
               {
                  ((MessagesExportType)target).setMessageListener(null);
                  messageCounter++;
               }
            }
         });

         SAXParserFactory factory = SAXParserFactory.newInstance();
         factory.setNamespaceAware(true);
         XMLReader reader = factory.newSAXParser().getXMLReader();
         reader.setContentHandler(unmarshaller.getUnmarshallerHandler());

         reader.parse(new InputSource(importFile));
      }
      finally
      {
         if (sf != null)
         {
            sf.close();
         }
      }

      return messageCounter;
   }

   private static Map<String, Long> loadBindings(final String bindingsDirectry) throws Exception
   {
      final Map<String, Long> queueMapping;

      JournalImpl bindingsJournal = null;
      try
      {

         SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDirectry);
         bindingsJournal = new JournalImpl(FileConfiguration.DEFAULT_JOURNAL_FILE_SIZE,
                                           FileConfiguration.DEFAULT_JOURNAL_MIN_FILES,
                                           0,
                                           0,
                                           bindingsFF,
                                           "hornetq-bindings",
                                           "bindings",
                                           1);

         bindingsJournal.start();
         List<RecordInfo> committedRecordsB = new ArrayList<RecordInfo>();
         List<PreparedTransactionInfo> preparedTxnB = new ArrayList<PreparedTransactionInfo>();
         bindingsJournal.load(committedRecordsB, preparedTxnB, null, false);

         queueMapping = new HashMap<String, Long>();

         for (RecordInfo bindingRecord : committedRecordsB)
         {
            if (bindingRecord.getUserRecordType() == JournalStorageManager.QUEUE_BINDING_RECORD)
            {
               // mapping old/new queue binding
               PersistentQueueBindingEncoding queueBinding = JournalStorageManager.newBindingEncoding(bindingRecord.id,
                                                                                                      HornetQBuffers.wrappedBuffer(bindingRecord.data));
               queueMapping.put(queueBinding.getAddress().toString(), queueBinding.getId());
            }
         }
      }
      finally
      {
         if (bindingsJournal != null)
         {
            bindingsJournal.stop();
         }
      }
      return queueMapping;
   }
}
