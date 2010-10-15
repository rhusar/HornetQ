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

package org.hornetq.core.paging;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.transaction.Transaction;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface PageTransactionInfo extends EncodingSupport
{
   boolean isCommit();

   boolean isRollback();

   void commit();

   void rollback();

   long getRecordID();

   void setRecordID(long id);

   long getTransactionID();
   
   void store(StorageManager storageManager, PagingManager pagingManager, Transaction tx) throws Exception;
   
   void storeUpdate(StorageManager storageManager, PagingManager pagingManager, Transaction tx, int depages) throws Exception;

   // To be used after the update was stored or reload
   void onUpdate(int update, StorageManager storageManager, PagingManager pagingManager);

   void increment();

   int getNumberOfMessages();
}
