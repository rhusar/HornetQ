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

package org.hornetq.core.config;

import java.io.Serializable;
import java.util.List;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.impl.ConfigurationImpl;

/**
 * A ClusterConnectionConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 13 Jan 2009 09:42:17
 *
 *
 */
public class ClusterConnectionConfiguration implements Serializable
{
   private static final long serialVersionUID = 8948303813427795935L;

   private final String name;

   private final String address;

   private final String connectorName;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final long maxRetryInterval;

   private final int reconnectAttempts;

   private final long callTimeout;

   private final boolean duplicateDetection;

   private final boolean forwardWhenNoConsumers;

   private final DiscoveryGroupConfiguration discoveryGroupConfiguration;

   private final int maxHops;

   private final int confirmationWindowSize;

   private final boolean allowDirectConnectionsOnly;

   private final List<TransportConfiguration> staticConnectors;

   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final long retryInterval,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final DiscoveryGroupConfiguration discoveryGroupConfig,
                                         boolean allowDirectConnectionsOnly)
   {
      this(name,
           address,
           connectorName,
           ConfigurationImpl.DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD,
           ConfigurationImpl.DEFAULT_CLUSTER_CONNECTION_TTL,
           retryInterval,
           ConfigurationImpl.DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER,
           ConfigurationImpl.DEFAULT_CLUSTER_MAX_RETRY_INTERVAL,
           ConfigurationImpl.DEFAULT_CLUSTER_RECONNECT_ATTEMPTS,
           HornetQClient.DEFAULT_CALL_TIMEOUT,
           duplicateDetection,
           forwardWhenNoConsumers,
           maxHops,
           confirmationWindowSize,
           discoveryGroupConfig,
           allowDirectConnectionsOnly,
           (List<TransportConfiguration>)discoveryGroupConfig.getParams()
                                                             .get(DiscoveryGroupConstants.STATIC_CONNECTOR_CONFIG_LIST_NAME));
   }


   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final long clientFailureCheckPeriod,
                                         final long connectionTTL,
                                         final long retryInterval,
                                         final double retryIntervalMultiplier,
                                         final long maxRetryInterval,
                                         final int reconnectAttempts,
                                         final long callTimeout,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final DiscoveryGroupConfiguration discoveryGroupConfig,
                                         boolean allowDirectConnectionsOnly,
                                         final List<TransportConfiguration> staticConnectors)
   {
      this.name = name;
      this.address = address;
      this.connectorName = connectorName;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.connectionTTL = connectionTTL;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetryInterval = maxRetryInterval;
      this.reconnectAttempts = reconnectAttempts;
      this.callTimeout = callTimeout;
      this.duplicateDetection = duplicateDetection;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      this.discoveryGroupConfiguration = discoveryGroupConfig;
      this.maxHops = maxHops;
      this.confirmationWindowSize = confirmationWindowSize;
      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;
      this.staticConnectors = staticConnectors;
   }

   public String getName()
   {
      return name;
   }

   public String getAddress()
   {
      return address;
   }

   /**
    * @return the clientFailureCheckPeriod
    */
   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   /**
    * @return the retryIntervalMultiplier
    */
   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   /**
    * @return the reconnectAttempts
    */
   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public String getConnectorName()
   {
      return connectorName;
   }

   public boolean isDuplicateDetection()
   {
      return duplicateDetection;
   }

   public boolean isForwardWhenNoConsumers()
   {
      return forwardWhenNoConsumers;
   }

   public int getMaxHops()
   {
      return maxHops;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public List<TransportConfiguration> getStaticConnectors()
   {
      return staticConnectors;
   }

   public DiscoveryGroupConfiguration getDiscoveryGroupConfiguration()
   {
      return discoveryGroupConfiguration;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public boolean isAllowDirectConnectionsOnly()
   {
      return allowDirectConnectionsOnly;
   }
}
