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

import java.util.List;
import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ClusterConnectionControl;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.utils.json.JSONArray;

/**
 * A ClusterConnectionControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClusterConnectionControlImpl extends AbstractControl implements ClusterConnectionControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClusterConnection clusterConnection;

   private final ClusterConnectionConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClusterConnectionControlImpl(final ClusterConnection clusterConnection,
                                       final StorageManager storageManager,
                                       final ClusterConnectionConfiguration configuration) throws Exception
   {
      super(ClusterConnectionControl.class, storageManager);
      this.clusterConnection = clusterConnection;
      this.configuration = configuration;
   }

   // ClusterConnectionControlMBean implementation ---------------------------

   public String getAddress()
   {
      clearIO();
      try
      {
         return configuration.getAddress();
      }
      finally
      {
         blockOnIO();
      }

   }

   public String getDiscoveryGroupName()
   {
      clearIO();
      try
      {
         return configuration.getDiscoveryGroupConfiguration().getName();
      }
      finally
      {
         blockOnIO();
      }

   }

   public int getMaxHops()
   {
      clearIO();
      try
      {
         return configuration.getMaxHops();
      }
      finally
      {
         blockOnIO();
      }

   }

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

   public long getRetryInterval()
   {
      clearIO();
      try
      {
         return configuration.getRetryInterval();
      }
      finally
      {
         blockOnIO();
      }

   }

   public String getNodeID()
   {
      clearIO();
      try
      {
         return clusterConnection.getNodeID();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getStaticConnectors()
   {
      clearIO();
      try
      {
         List<TransportConfiguration> connectors =
                  (List<TransportConfiguration>)configuration
                  .getDiscoveryGroupConfiguration()
                  .getParams()
                  .get(DiscoveryGroupConstants.STATIC_CONNECTOR_CONFIG_LIST_NAME);
         if (connectors == null)
         {
            return null;
         }

         String[] array = new String[connectors.size()];

         for (int i=0; i<connectors.size(); i++)
         {
            array[i] = connectors.get(i).getName();
         }
         return array;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getStaticConnectorsAsJSON() throws Exception
   {
      clearIO();
      try
      {
         List<TransportConfiguration> connectors =
                  (List<TransportConfiguration>)configuration
                  .getDiscoveryGroupConfiguration()
                  .getParams()
                  .get(DiscoveryGroupConstants.STATIC_CONNECTOR_CONFIG_LIST_NAME);

         if (connectors == null)
         {
            return null;
         }

         JSONArray array = new JSONArray();

         for (TransportConfiguration connector : connectors)
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

   public boolean isDuplicateDetection()
   {
      clearIO();
      try
      {
         return configuration.isDuplicateDetection();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isForwardWhenNoConsumers()
   {
      clearIO();
      try
      {
         return configuration.isForwardWhenNoConsumers();
      }
      finally
      {
         blockOnIO();
      }
   }

   public Map<String, String> getNodes() throws Exception
   {
      clearIO();
      try
      {
         return clusterConnection.getNodes();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isStarted()
   {
      clearIO();
      try
      {
         return clusterConnection.isStarted();
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
         clusterConnection.start();
         clusterConnection.flushExecutor();
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
         clusterConnection.stop();
         clusterConnection.flushExecutor();
      }
      finally
      {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(ClusterConnectionControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
