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

package org.hornetq.integration.discovery.jgroups;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.server.cluster.impl.ClusterConnectionImpl;
import org.hornetq.core.server.cluster.impl.ClusterConnector;
/**
 * A JGroupsDiscoveryClusterConnectorImpl
 *
 * @author <a href=\"tm.igarashi@gmail.com\">Tomohisa Igarashi</a>
 */
class JGroupsDiscoveryClusterConnectorImpl implements ClusterConnector
{
   private final ClusterConnectionImpl clusterConnectionImpl;
   private final DiscoveryGroupConfiguration dg;

   public JGroupsDiscoveryClusterConnectorImpl(ClusterConnectionImpl clusterConnectionImpl, DiscoveryGroupConfiguration dg)
   {
      this.clusterConnectionImpl = clusterConnectionImpl;
      this.dg = dg;
   }

   @Override
   public ServerLocatorInternal createServerLocator(boolean includeTopology)
   {
      JGroupsServerLocatorImpl locator = new JGroupsServerLocatorImpl(includeTopology?this.clusterConnectionImpl.getTopology():null, true, dg);
      return locator;

   }
}