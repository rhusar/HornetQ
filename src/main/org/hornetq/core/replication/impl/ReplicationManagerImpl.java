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

import org.hornetq.core.replication.BackupListener;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.replication.ReplicationToken;

/**
 * A RepplicationManagerImpl
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationManagerImpl implements ReplicationManager
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#addListener(org.hornetq.core.replication.ReplicationListener)
    */
   public void addBackupListener(BackupListener listener)
   {
      // TODO Auto-generated method stub
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#replicate(byte[], org.hornetq.core.replication.ReplicationToken)
    */
   public void replicate(byte[] bytes, ReplicationToken token)
   {
      // TODO Auto-generated method stub
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
