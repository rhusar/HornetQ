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

package org.hornetq.jms.persistence.impl.journal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.replication.impl.ReplicatedJournal;
import org.hornetq.core.server.JournalType;
import org.hornetq.jms.persistence.JMSStorageManager;
import org.hornetq.jms.persistence.PersistedConnectionFactory;
import org.hornetq.jms.persistence.PersistedDestination;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.utils.IDGenerator;

/**
 * A JournalJMSStorageManagerImpl
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalJMSStorageManagerImpl implements JMSStorageManager
{

   // Constants -----------------------------------------------------

   private final byte CF_RECORD = 1;

   private final byte DESTINATION_RECORD = 2;
   
   // Attributes ----------------------------------------------------

   private final IDGenerator idGenerator;
   
   private final String bindingsDir;
   
   private final boolean createBindingsDir;
   
   private final Journal jmsJournal;

   private volatile boolean started;
   
   private Map<String, PersistedConnectionFactory> mapFactories = new ConcurrentHashMap<String, PersistedConnectionFactory>();

   private Map<String, PersistedDestination> destinations = new ConcurrentHashMap<String, PersistedDestination>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public JournalJMSStorageManagerImpl(final IDGenerator idGenerator,
                                       final Configuration config,
                                final ReplicationManager replicator)
   {
      if (config.getJournalType() != JournalType.NIO && config.getJournalType() != JournalType.ASYNCIO)
      {
         throw new IllegalArgumentException("Only NIO and AsyncIO are supported journals");
      }

      bindingsDir = config.getBindingsDirectory();

      if (bindingsDir == null)
      {
         throw new NullPointerException("bindings-dir is null");
      }

      createBindingsDir = config.isCreateBindingsDir();

      SequentialFileFactory bindingsJMS = new NIOSequentialFileFactory(bindingsDir);

      Journal localJMS = new JournalImpl(1024 * 1024,
                                              2,
                                              config.getJournalCompactMinFiles(),
                                              config.getJournalCompactPercentage(),
                                              bindingsJMS,
                                              "hornetq-jms-config",
                                              "jms",
                                              1);

      if (replicator != null)
      {
         jmsJournal = new ReplicatedJournal((byte)2, localJMS, replicator);
      }
      else
      {
         jmsJournal = localJMS;
      }
      
      this.idGenerator = idGenerator;
   }


   // Public --------------------------------------------------------
   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#recoverConnectionFactories()
    */
   public List<PersistedConnectionFactory> recoverConnectionFactories()
   {
      List<PersistedConnectionFactory> cfs = new ArrayList<PersistedConnectionFactory>(mapFactories.size());
      cfs.addAll(mapFactories.values());
      return cfs;
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#storeConnectionFactory(org.hornetq.jms.persistence.PersistedConnectionFactory)
    */
   public void storeConnectionFactory(final PersistedConnectionFactory connectionFactory) throws Exception
   {
      deleteConnectionFactory(connectionFactory.getName());
      long id = idGenerator.generateID();
      connectionFactory.setId(id);
      jmsJournal.appendAddRecord(id, CF_RECORD, connectionFactory, true);
      mapFactories.put(connectionFactory.getName(), connectionFactory);
   }
   
   public void deleteConnectionFactory(final String cfName) throws Exception
   {
      PersistedConnectionFactory oldCF = mapFactories.remove(cfName);
      if (oldCF != null)
      {
         jmsJournal.appendDeleteRecord(oldCF.getId(), false);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#recoverDestinations()
    */
   public List<PersistedDestination> recoverDestinations()
   {
      List<PersistedDestination> destinations = new ArrayList<PersistedDestination>(this.destinations.size());
      destinations.addAll(this.destinations.values());
      return destinations;
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#storeDestination(org.hornetq.jms.persistence.PersistedDestination)
    */
   public void storeDestination(final PersistedDestination destination) throws Exception
   {
      deleteDestination(destination.getName());
      long id = idGenerator.generateID();
      destination.setId(id);
      jmsJournal.appendAddRecord(id, DESTINATION_RECORD, destination, true);
      destinations.put(destination.getName(), destination);
   }

   public void deleteDestination(final String name) throws Exception
   {
      PersistedDestination destination = destinations.get(name);
      if(destination != null)
      {
         jmsJournal.appendDeleteRecord(destination.getId(), false);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return started;
   }


   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {

      checkAndCreateDir(bindingsDir, createBindingsDir);

      jmsJournal.start();
      
      load();
      
      started = true;
   }


   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      jmsJournal.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void load() throws Exception
   {
      mapFactories.clear();
      
      List<RecordInfo> data = new ArrayList<RecordInfo>();
      
      ArrayList<PreparedTransactionInfo> list = new ArrayList<PreparedTransactionInfo>();
      
      jmsJournal.load(data, list, null);
      
      for (RecordInfo record : data)
      {
         long id = record.id;

         HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(record.data);

         byte rec = record.getUserRecordType();
         
         if (rec == CF_RECORD)
         {
            PersistedConnectionFactory cf = new PersistedConnectionFactory();
            cf.decode(buffer);
            cf.setId(id);
            mapFactories.put(cf.getName(), cf);
         }
         else if(rec == DESTINATION_RECORD)
         {
            PersistedDestination destination = new PersistedDestination();
            destination.decode(buffer);
            destination.setId(id);
            destinations.put(destination.getName(), destination);
         }
         else
         {
            throw new IllegalStateException("Invalid record type " + rec);
         }
         
      }
      
   }

   private void checkAndCreateDir(final String dir, final boolean create)
   {
      File f = new File(dir);

      if (!f.exists())
      {
         if (create)
         {
            if (!f.mkdirs())
            {
               throw new IllegalStateException("Failed to create directory " + dir);
            }
         }
         else
         {
            throw new IllegalArgumentException("Directory " + dir + " does not exist and will not create it");
         }
      }
   }

   // Inner classes -------------------------------------------------

}
