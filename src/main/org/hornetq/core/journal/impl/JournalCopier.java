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

package org.hornetq.core.journal.impl;

import java.util.Set;

import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;

/**
 * This will read records 
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCopier extends AbstractJournalUpdateTask
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JournalCopier.class);

   /** enable some trace at development. */
   private static final boolean DEV_TRACE = true;

   private static final boolean isTraceEnabled = log.isTraceEnabled();

   private static void trace(final String msg)
   {
      System.out.println("JournalCopier::" + msg);
   }

   // Attributes ----------------------------------------------------

   private final Set<Long> pendingTransactions;

   private final Journal journalTo;

   /** Proxy mode means, everything will be copied over without any evaluations such as */
   private boolean proxyMode = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param fileFactory
    * @param journal
    * @param recordsSnapshot
    * @param nextOrderingID
    */
   public JournalCopier(final SequentialFileFactory fileFactory,
                        final JournalImpl journalFrom,
                        final Journal journalTo,
                        final Set<Long> recordsSnapshot,
                        final Set<Long> pendingTransactionsSnapshot)
   {
      super(fileFactory, journalFrom, recordsSnapshot, -1);
      pendingTransactions = pendingTransactionsSnapshot;
      this.journalTo = journalTo;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.impl.JournalReaderCallback#onReadAddRecord(org.hornetq.core.journal.RecordInfo)
    */

   public void onReadAddRecord(final RecordInfo info) throws Exception
   {
      if (proxyMode || lookupRecord(info.id))
      {
         if (DEV_TRACE)
         {
            trace("Backing add ID = " + info.id);
         }
         journalTo.appendAddRecord(info.id, info.userRecordType, info.data, false);
      }
      else
      {
         if (DEV_TRACE)
         {
            trace("Ignoring add ID = " + info.id);
         }
      }
   }

   public void onReadAddRecordTX(final long transactionID, final RecordInfo info) throws Exception
   {
      if (proxyMode || pendingTransactions.contains(transactionID))
      {
         if (DEV_TRACE)
         {
            trace("Backing add TXID = " + transactionID + " id = " + info.id);
         }
         journalTo.appendAddRecordTransactional(transactionID, info.id, info.userRecordType, info.data);
      }
      else
      {
         // Will try it as a regular record, the method addRecord will validate if this is a live record or not
         onReadAddRecord(info);
      }
   }

   public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
   {

      if (proxyMode)
      {
         journalTo.appendCommitRecord(transactionID, false);
      }
      else if (pendingTransactions.contains(transactionID))
      {
         // Sanity check, this should never happen
         log.warn("Inconsistency during compacting: CommitRecord ID = " + transactionID +
                  " for an already committed transaction during compacting");
      }
   }

   public void onReadDeleteRecord(final long recordID) throws Exception
   {
      if (proxyMode)
      {
         journalTo.appendDeleteRecord(recordID, false);
      }
      // else....
      // Nothing to be done here, we don't copy deleted records
   }

   public void onReadDeleteRecordTX(final long transactionID, final RecordInfo info) throws Exception
   {
      if (proxyMode || pendingTransactions.contains(transactionID))
      {
         journalTo.appendDeleteRecordTransactional(transactionID, info.id, info.data);
      }
      // else.. nothing to be done
   }

   public void markAsDataFile(final JournalFile file)
   {
      // nothing to be done here
   }

   public void onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords) throws Exception
   {
      if (proxyMode || pendingTransactions.contains(transactionID))
      {
         journalTo.appendPrepareRecord(transactionID, extraData, false);
      }
   }

   public void onReadRollbackRecord(final long transactionID) throws Exception
   {
      if (proxyMode)
      {
         journalTo.appendRollbackRecord(transactionID, false);
      }
      if (pendingTransactions.contains(transactionID))
      {
         // Sanity check, this should never happen
         log.warn("Inconsistency during copying: RollbackRecord ID = " + transactionID +
                  " for an already rolled back transaction during compacting");
      }
   }

   public void onReadUpdateRecord(final RecordInfo info) throws Exception
   {
      if (proxyMode || lookupRecord(info.id))
      {
         journalTo.appendUpdateRecord(info.id, info.userRecordType, info.data, false);
      }
   }

   public void onReadUpdateRecordTX(final long transactionID, final RecordInfo info) throws Exception
   {
      if (proxyMode || pendingTransactions.contains(transactionID))
      {
         journalTo.appendUpdateRecordTransactional(transactionID, info.id, info.userRecordType, info.data);
      }
      else
      {
         onReadUpdateRecord(info);
      }
   }

   public void setProxyMode(final boolean proxyMode)
   {
      this.proxyMode = proxyMode;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
