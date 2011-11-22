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

package org.hornetq.core.server.cluster.impl;

import java.util.List;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.StaticServerLocatorImpl;

/**
 * A StaticClusterConnectorImpl
 */
class StaticClusterConnectorImpl implements ClusterConnector
{
   private final ClusterConnectionImpl clusterConnectionImpl;
   private final List<TransportConfiguration> tcConfigs;

   public StaticClusterConnectorImpl(ClusterConnectionImpl clusterConnectionImpl, DiscoveryGroupConfiguration dg)
   {
      this.clusterConnectionImpl = clusterConnectionImpl;
      this.tcConfigs = (List<TransportConfiguration>)dg.getParams().get(DiscoveryGroupConstants.STATIC_CONNECTOR_CONFIG_LIST_NAME);
   }

   @Override
   public ServerLocatorInternal createServerLocator(boolean includeTopology)
   {
      if (tcConfigs != null && tcConfigs.size() > 0)
      {
         if (ClusterConnectionImpl.log.isDebugEnabled())
         {
            ClusterConnectionImpl.log.debug(this.clusterConnectionImpl + "Creating a serverLocator for " + tcConfigs);
         }
         StaticServerLocatorImpl locator = new StaticServerLocatorImpl(includeTopology ? this.clusterConnectionImpl.topology : null,
                                                                                       true,
                                                                                       tcConfigs.toArray(new TransportConfiguration[0]));
         locator.setClusterConnection(true);
         return locator;
      }
      else
      {
         return null;
      }
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "StaticClusterConnector [tcConfigs=" + tcConfigs + "]";
   }

}