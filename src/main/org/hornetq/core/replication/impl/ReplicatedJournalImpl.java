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
import org.hornetq.core.journal.impl.JournalImpl.ByteArrayEncoding;
import org.hornetq.core.replication.ReplicationManager;

/**
 * A ReplicatedJournalImpl
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicatedJournalImpl implements Journal
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ReplicationManager replicationManager;

   private final Journal replicatedJournal;

   private final byte journalID;

   /**
    * @param journaID
    * @param replicatedJournal
    * @param replicationManager
    */
   public ReplicatedJournalImpl(byte journaID, Journal replicatedJournal, ReplicationManager replicationManager)
   {
      super();
      this.journalID = journaID;
      this.replicatedJournal = replicatedJournal;
      this.replicationManager = replicationManager;
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean)
    */
   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      replicationManager.appendAddRecord(journalID, id, recordType, new ByteArrayEncoding(record));
      replicatedJournal.appendAddRecord(id, recordType, record, sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      replicationManager.appendAddRecord(journalID, id, recordType, record);
      replicatedJournal.appendAddRecord(id, recordType, record, sync);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, byte[])
    */
   public void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
   {
      replicationManager.appendAddRecordTransactional(journalID, txID, id, recordType, new ByteArrayEncoding(record));
      replicatedJournal.appendAddRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception
   {
      replicationManager.appendAddRecordTransactional(journalID, txID, id, recordType, record);
      replicatedJournal.appendAddRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean)
    */
   public void appendCommitRecord(long txID, boolean sync) throws Exception
   {
      replicationManager.appendCommitRecord(journalID, txID);
      replicatedJournal.appendCommitRecord(txID, sync);
   }

   /**
    * @param id
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean)
    */
   public void appendDeleteRecord(long id, boolean sync) throws Exception
   {
      replicationManager.appendDeleteRecord(journalID, id);
      replicatedJournal.appendDeleteRecord(id, sync);
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, byte[])
    */
   public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception
   {
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id, new ByteArrayEncoding(record));
      replicatedJournal.appendDeleteRecordTransactional(txID, id, record);
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception
   {
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id, record);
      replicatedJournal.appendDeleteRecordTransactional(txID, id, record);
   }

   /**
    * @param txID
    * @param id
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long)
    */
   public void appendDeleteRecordTransactional(long txID, long id) throws Exception
   {
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id);
      replicatedJournal.appendDeleteRecordTransactional(txID, id);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, byte[], boolean)
    */
   public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception
   {
      replicationManager.appendPrepareRecord(journalID, txID, new ByteArrayEncoding(transactionData));
      replicatedJournal.appendPrepareRecord(txID, transactionData, sync);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception
   {
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      replicatedJournal.appendPrepareRecord(txID, transactionData, sync);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean)
    */
   public void appendRollbackRecord(long txID, boolean sync) throws Exception
   {
      replicationManager.appendRollbackRecord(journalID, txID);
      replicatedJournal.appendRollbackRecord(txID, sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean)
    */
   public void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      replicationManager.appendUpdateRecord(journalID, id, recordType, new ByteArrayEncoding(record));
      replicatedJournal.appendUpdateRecord(id, recordType, record, sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      replicationManager.appendUpdateRecord(journalID, id, recordType, record);
      replicatedJournal.appendUpdateRecord(id, recordType, record, sync);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, byte[])
    */
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception
   {
      replicationManager.appendUpdateRecordTransactional(journalID, txID, id, recordType, new ByteArrayEncoding(record));
      replicatedJournal.appendUpdateRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception
   {
      replicationManager.appendUpdateRecordTransactional(journalID, txID, id, recordType, record);
      replicatedJournal.appendUpdateRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param committedRecords
    * @param preparedTransactions
    * @param transactionFailure
    * @return
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(java.util.List, java.util.List, org.hornetq.core.journal.TransactionFailureCallback)
    */
   public long load(List<RecordInfo> committedRecords,
                    List<PreparedTransactionInfo> preparedTransactions,
                    TransactionFailureCallback transactionFailure) throws Exception
   {
      return replicatedJournal.load(committedRecords, preparedTransactions, transactionFailure);
   }

   /**
    * @param reloadManager
    * @return
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(org.hornetq.core.journal.LoaderCallback)
    */
   public long load(LoaderCallback reloadManager) throws Exception
   {
      return replicatedJournal.load(reloadManager);
   }

   /**
    * @param pages
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#perfBlast(int)
    */
   public void perfBlast(int pages) throws Exception
   {
      replicatedJournal.perfBlast(pages);
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      replicatedJournal.start();
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      replicatedJournal.stop();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#getAlignment()
    */
   public int getAlignment() throws Exception
   {
      return replicatedJournal.getAlignment();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return replicatedJournal.isStarted();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
