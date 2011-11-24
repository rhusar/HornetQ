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

package org.hornetq.core.management.impl;

import javax.management.MBeanOperationInfo;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.BroadcastGroupControl;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.BroadcastGroupConstants;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.json.JSONArray;

/**
 * A BroadcastGroupControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:09:04
 */
public class BroadcastGroupControlImpl extends AbstractControl implements BroadcastGroupControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final BroadcastGroup broadcastGroup;

   private final BroadcastGroupConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BroadcastGroupControlImpl(final BroadcastGroup broadcastGroup,
                                    final StorageManager storageManager,
                                    final BroadcastGroupConfiguration configuration) throws Exception
   {
      super(BroadcastGroupControl.class, storageManager);
      this.broadcastGroup = broadcastGroup;
      this.configuration = configuration;
   }

   // BroadcastGroupControlMBean implementation ---------------------

   public String getName()
   {
      clearIO();
      try
      {
         return configuration.getName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public long getBroadcastPeriod()
   {
      clearIO();
      try
      {
         return ConfigurationHelper.getLongProperty(BroadcastGroupConstants.BROADCAST_PERIOD_NAME,
                                                             -1,
                                                    configuration.getParams());
      }
      finally
      {
         blockOnIO();
      }
   }

   public Object[] getConnectorPairs()
   {
      clearIO();
      try
      {
         Object[] ret = new Object[configuration.getConnectorList().size()];

         int i = 0;
         for (TransportConfiguration connector : configuration.getConnectorList())
         {
            ret[i++] = connector.getName();
         }

         return ret;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getConnectorPairsAsJSON() throws Exception
   {
      clearIO();
      try
      {
         JSONArray array = new JSONArray();

         for (TransportConfiguration connector : configuration.getConnectorList())
         {
            array.put(connector.getName());
         }
         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getGroupAddress()
   {
      clearIO();
      try
      {
         return ConfigurationHelper.getStringProperty(BroadcastGroupConstants.GROUP_ADDRESS_NAME,
                                                      null,
                                                      configuration.getParams());
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getGroupPort()
   {
      clearIO();
      try
      {
         return ConfigurationHelper.getIntProperty(BroadcastGroupConstants.GROUP_PORT_NAME,
                                                            -1,
                                                   configuration.getParams());
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getLocalBindPort()
   {
      clearIO();
      try
      {
         return ConfigurationHelper.getIntProperty(BroadcastGroupConstants.LOCAL_BIND_PORT_NAME,
                                                            -1,
                                                   configuration.getParams());
      }
      finally
      {
         blockOnIO();
      }
   }

   // MessagingComponentControlMBean implementation -----------------

   public boolean isStarted()
   {
      clearIO();
      try
      {
         return broadcastGroup.isStarted();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void start() throws Exception
   {
      clearIO();
      try
      {
         broadcastGroup.start();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void stop() throws Exception
   {
      clearIO();
      try
      {
         broadcastGroup.stop();
      }
      finally
      {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(BroadcastGroupControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
