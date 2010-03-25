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

package org.hornetq.jms.persistence;

import java.util.List;

import org.hornetq.core.server.HornetQComponent;
import org.hornetq.jms.persistence.impl.PersistedJNDIBinding;

/**
 * A JMSPersistence
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface JMSStorageManager extends HornetQComponent
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   void storeDestination(PersistedDestination destination) throws Exception;

   void deleteDestination(String name) throws Exception;
   
   List<PersistedDestination> recoverDestinations();
   
   void deleteConnectionFactory(String connectionFactory) throws Exception;
   
   void storeConnectionFactory(PersistedConnectionFactory connectionFactory) throws Exception;
   
   List<PersistedConnectionFactory> recoverConnectionFactories();
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   void storeJndiBinding(PersistedJNDIBinding persistedJNDIBinding);
}
