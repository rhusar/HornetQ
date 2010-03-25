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

package org.hornetq.jms.persistence.impl.nullpm;

import java.util.Collections;
import java.util.List;

import org.hornetq.jms.persistence.JMSStorageManager;
import org.hornetq.jms.persistence.PersistedConnectionFactory;
import org.hornetq.jms.persistence.PersistedDestination;

/**
 * A NullJMSStorageManagerImpl
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class NullJMSStorageManagerImpl implements JMSStorageManager
{

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#deleteConnectionFactory(java.lang.String)
    */
   public void deleteConnectionFactory(String connectionFactory) throws Exception
   {
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#recoverConnectionFactories()
    */
   public List<PersistedConnectionFactory> recoverConnectionFactories()
   {
      return Collections.emptyList();
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#recoverDestinations()
    */
   public List<PersistedDestination> recoverDestinations()
   {
      return Collections.emptyList();
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#storeConnectionFactory(org.hornetq.jms.persistence.PersistedConnectionFactory)
    */
   public void storeConnectionFactory(PersistedConnectionFactory connectionFactory) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#storeDestination(org.hornetq.jms.persistence.PersistedDestination)
    */
   public void storeDestination(PersistedDestination destination)
   {
   }

    /* (non-Javadoc)
    * @see org.hornetq.jms.persistence.JMSStorageManager#deleteDestination (org.hornetq.jms.persistence.PersistedDestination)
    */
   public void deleteDestination(String name) throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
