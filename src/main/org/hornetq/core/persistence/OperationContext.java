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

package org.hornetq.core.persistence;

import java.util.concurrent.Executor;

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.IOCompletion;


/**
 * This represents a set of operations done as part of replication. 
 * When the entire set is done a group of Runnables can be executed.
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface OperationContext extends IOCompletion
{
   
   boolean hasReplication();
   
   /** Reattach the context to the current thread */
   void reinstall();
   
   /** The executor used on responses.
    *  If this is not set, it will use the current thread. */
   void setExecutor(Executor executor);

   /** Execute the task when all IO operations are complete,
    *  Or execute it immediately if nothing is pending.  */
   void executeOnCompletion(IOAsyncTask runnable);
   
   void replicationLineUp();
   
   void replicationDone();

   /** To be called when there are no more operations pending */
   void complete();
   
   /** Is this a special operation to sync replication. */
   boolean isSync();

}
