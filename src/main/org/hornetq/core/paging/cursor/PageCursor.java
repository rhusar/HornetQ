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

package org.hornetq.core.paging.cursor;

import org.hornetq.api.core.Pair;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;

/**
 * A PageCursor
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 *
 */
public interface PageCursor
{

   // Cursor query operations --------------------------------------
   
   Pair<PagePosition, ServerMessage> moveNext() throws Exception;

   void ack(PagePosition position) throws Exception;

   void ackTx(Transaction tx, PagePosition position) throws Exception;
   
   // Reload operations
   
   /**
    * @param position
    */
   void reloadACK(PagePosition position);
   
   /**
    * To be used to avoid a redelivery of a prepared ACK after load
    * @param position
    */
   void reloadPreparedACK(Transaction tx, PagePosition position);
   
   void processReload();

   /**
    * To be used on redeliveries
    * @param position
    */
   void redeliver(PagePosition position);
}
