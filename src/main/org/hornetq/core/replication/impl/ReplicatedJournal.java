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

package org.hornetq.core.replication.impl;

import java.util.List;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalLock;
import org.hornetq.core.journal.impl.JournalImpl.ByteArrayEncoding;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.replication.ReplicationManager;

/**
 * Used by the {@link JournalStorageManager} to replicate journal calls. 
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @see JournalStorageManager
 *
 */
public class ReplicatedJournal implements Journal
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatedJournal.class);

   // Attributes ----------------------------------------------------

   private static final boolean trace = log.isTraceEnabled();

   private volatile ReplicationManager replicationManager;

   private final Journal localJournal;

   private final JournalLock journalLock;

   private final byte journalID;

   /** The server is started with the backup */
   public ReplicatedJournal(final byte journaID,
                            final JournalLock journalLock,
                            final Journal localJournal,
                            final ReplicationManager replicationManager)
   {
      super();
      journalID = journaID;
      this.localJournal = localJournal;
      this.replicationManager = replicationManager;
      this.journalLock = journalLock;
   }

   /** Used by the backup process, to send the backup over */
   public ReplicatedJournal(final byte journaID, final ReplicationManager replicationManager)
   {
      super();
      journalID = journaID;
      localJournal = null;
      journalLock = null;
      this.replicationManager = replicationManager;
   }

   // Static --------------------------------------------------------

   private static void trace(final String message)
   {
      log.trace(message);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * @param replication
    */
   public void initiateReplication(final ReplicationManager replication) throws Exception
   {
      if (replicationManager != null)
      {
         journalLock.writeLock();
         try
         {
            replicationManager = null;
         }
         finally
         {
            journalLock.writeUnLock();
         }
      }
      
      // Instantiate a new Replicated Journal that won't have any local journal associated.
      // This will happen in parallele while the current journal still being used.
      ReplicatedJournal proxy = new ReplicatedJournal(this.journalID,
                                                  null,
                                                  null,
                                                  replication);
      
      // TODO: the connection used here should have a hook to enable nagling
      
      localJournal.copyTo(proxy, new Runnable()
      {
         public void run()
         {
            // This needs to be done right after the copy is finished
            // But while the journal still locked on its final stage, 
            // so after this point all the journal appends are going to be replicated
            ReplicatedJournal.this.replicationManager = replication;
         }
      });
   }

   // Journal implementation ----------------------------------------
   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean)
    */
   public void appendAddRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
   {
      this.appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("Append record id = " + id + " recordType = " + recordType);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendAddRecord(journalID, id, recordType, record);
         }
         if (localJournal != null)
         {
            localJournal.appendAddRecord(id, recordType, record, sync);
         }
      }
      finally
      {
         afterAppend();
      }
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, byte[])
    */
   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, final byte[] record) throws Exception
   {
      this.appendAddRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendAddRecordTransactional(final long txID,
                                            final long id,
                                            final byte recordType,
                                            final EncodingSupport record) throws Exception
   {
      if (trace)
      {
         trace("Append record TXid = " + id + " recordType = " + recordType);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendAddRecordTransactional(journalID, txID, id, recordType, record);
         }
         if (localJournal != null)
         {
            localJournal.appendAddRecordTransactional(txID, id, recordType, record);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean)
    */
   public void appendCommitRecord(final long txID, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendCommit " + txID);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendCommitRecord(journalID, txID);
         }
         if (localJournal != null)
         {
            localJournal.appendCommitRecord(txID, sync);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param id
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean)
    */
   public void appendDeleteRecord(final long id, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendDelete " + id);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendDeleteRecord(journalID, id);
         }
         if (localJournal != null)
         {
            localJournal.appendDeleteRecord(id, sync);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, byte[])
    */
   public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
   {
      this.appendDeleteRecordTransactional(txID, id, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record) throws Exception
   {
      if (trace)
      {
         trace("AppendDelete txID=" + txID + " id=" + id);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendDeleteRecordTransactional(journalID, txID, id, record);
         }
         if (localJournal != null)
         {
            localJournal.appendDeleteRecordTransactional(txID, id, record);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param txID
    * @param id
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long)
    */
   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      if (trace)
      {
         trace("AppendDelete (noencoding) txID=" + txID + " id=" + id);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendDeleteRecordTransactional(journalID, txID, id);
         }
         if (localJournal != null)
         {
            localJournal.appendDeleteRecordTransactional(txID, id);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, byte[], boolean)
    */
   public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception
   {
      this.appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendPrepare txID=" + txID);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendPrepareRecord(journalID, txID, transactionData);
         }
         if (localJournal != null)
         {
            localJournal.appendPrepareRecord(txID, transactionData, sync);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean)
    */
   public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendRollback " + txID);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendRollbackRecord(journalID, txID);
         }
         if (localJournal != null)
         {
            localJournal.appendRollbackRecord(txID, sync);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean)
    */
   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
   {
      this.appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendUpdateRecord id = " + id + " , recordType = " + recordType);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendUpdateRecord(journalID, id, recordType, record);
         }
         if (localJournal != null)
         {
            localJournal.appendUpdateRecord(id, recordType, record, sync);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, byte[])
    */
   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final byte[] record) throws Exception
   {
      this.appendUpdateRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final EncodingSupport record) throws Exception
   {
      if (trace)
      {
         trace("AppendUpdateRecord txid=" + txID + " id = " + id + " , recordType = " + recordType);
      }
      preAppend();
      try
      {
         if (replicationManager != null)
         {
            replicationManager.appendUpdateRecordTransactional(journalID, txID, id, recordType, record);
         }
         if (localJournal != null)
         {
            localJournal.appendUpdateRecordTransactional(txID, id, recordType, record);
         }
      }
      finally
      {
         afterAppend();
      }

   }

   /**
    * @param committedRecords
    * @param preparedTransactions
    * @param transactionFailure
    * @return
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(java.util.List, java.util.List, org.hornetq.core.journal.TransactionFailureCallback)
    */
   public long load(final List<RecordInfo> committedRecords,
                    final List<PreparedTransactionInfo> preparedTransactions,
                    final TransactionFailureCallback transactionFailure) throws Exception
   {
      if (localJournal != null)
      {
         return localJournal.load(committedRecords, preparedTransactions, transactionFailure);
      }
      else
      {
         return -1;
      }
   }

   /**
    * @param reloadManager
    * @return
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(org.hornetq.core.journal.LoaderCallback)
    */
   public long load(final LoaderCallback reloadManager) throws Exception
   {
      if (localJournal != null)
      {
         return localJournal.load(reloadManager);
      }
      else
      {
         return -1;
      }
   }

   /**
    * @param pages
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#perfBlast(int)
    */
   public void perfBlast(final int pages) throws Exception
   {
      if (localJournal != null)
      {
         localJournal.perfBlast(pages);
      }
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      if (localJournal != null)
      {
         localJournal.start();
      }
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      if (localJournal != null)
      {
         localJournal.stop();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#getAlignment()
    */
   public int getAlignment() throws Exception
   {
      if (localJournal != null)
      {
         return localJournal.getAlignment();
      }
      else
      {
         return 1;
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      if (localJournal != null)
      {
         return localJournal.isStarted();
      }
      else
      {
         return true;
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#copyTo(org.hornetq.core.journal.Journal)
    */
   public void copyTo(final Journal destJournal, Runnable aferCopy) throws Exception
   {
      // This would be a nonsense operation. Only the real journal can copyTo
      throw new IllegalStateException("Operation Not Implemeted!");
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#flush()
    */
   public void flush() throws Exception
   {
      if (localJournal != null)
      {
         localJournal.flush();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#loadInternalOnly()
    */
   public void loadInternalOnly() throws Exception
   {
      if (localJournal != null)
      {
         localJournal.loadInternalOnly();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void preAppend()
   {
      if (journalLock != null)
      {
         journalLock.readLock();
      }
   }

   private void afterAppend()
   {
      if (journalLock != null)
      {
         journalLock.readUnlock();
      }
   }

   // Inner classes -------------------------------------------------

}
