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

package org.hornetq.api.core;

import java.io.Serializable;
import java.util.Map;

import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.UUIDGenerator;

/**
 * A DiscoveryGroupConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:47:30
 *
 *
 */
public class DiscoveryGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 8657206421727863400L;
   
   private static final Logger log = Logger.getLogger(DiscoveryGroupConfiguration.class);


   private final String name;
   
   private final String serverLocatorClassName;

   private final Map<String, Object> params;

   public DiscoveryGroupConfiguration(final String clazz, final Map<String, Object> params, final String name)
   {
      this.serverLocatorClassName = clazz;
      
      this.params = params;
      
      this.name = name;
   }

   public String getServerLocatorClassName()
   {
      return this.serverLocatorClassName;
   }
   
   public Map<String, Object> getParams()
   {
      return this.params;
   }
  
   public String getName()
   {
      return this.name;
   }
   
   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DiscoveryGroupConfiguration that = (DiscoveryGroupConfiguration) o;

      if (this.params.get(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME)
               != that.params.get(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME)) return false;
      if (this.params.get(DiscoveryGroupConstants.GROUP_PORT_NAME)
               != that.params.get(DiscoveryGroupConstants.GROUP_PORT_NAME)) return false;
      if (this.params.get(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME)
               != that.params.get(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME)) return false;
      if (this.params.get(DiscoveryGroupConstants.GROUP_ADDRESS_NAME) != null
               ? !this.params.get(DiscoveryGroupConstants.GROUP_ADDRESS_NAME).equals(that.params.get(DiscoveryGroupConstants.GROUP_ADDRESS_NAME))
               : that.params.get(DiscoveryGroupConstants.GROUP_ADDRESS_NAME) != null) return false;
      if (this.params.get(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME) != null
               ? !this.params.get(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME).equals(that.params.get(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME))
               : that.params.get(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME) != null)
         return false;
      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      int groupPort = this.params.get(DiscoveryGroupConstants.GROUP_PORT_NAME) != null
               ? Integer.parseInt((String)this.params.get(DiscoveryGroupConstants.GROUP_PORT_NAME)) : 0;
      int refreshTimeout = this.params.get(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME) != null
               ? Integer.parseInt((String)this.params.get(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME)) : 0;
      int discoveryInitialWaitTimeout = this.params.get(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME) != null
               ? Integer.parseInt((String)this.params.get(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME)) : 0;
               
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (this.params.get(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME) != null ? this.params.get(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME).hashCode() : 0);
      result = 31 * result + (this.params.get(DiscoveryGroupConstants.GROUP_ADDRESS_NAME) != null ? this.params.get(DiscoveryGroupConstants.GROUP_ADDRESS_NAME).hashCode() : 0);
      result = 31 * result + groupPort;
      result = 31 * result + (int) (refreshTimeout ^ (refreshTimeout >>> 32));
      result = 31 * result + (int) (discoveryInitialWaitTimeout ^ (discoveryInitialWaitTimeout >>> 32));
      return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "DiscoveryGroupConfiguration [discoveryInitialWaitTimeout=" +
             this.params.get(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME) +
             ", groupAddress=" +
             this.params.get(DiscoveryGroupConstants.GROUP_ADDRESS_NAME) +
             ", groupPort=" +
             this.params.get(DiscoveryGroupConstants.GROUP_PORT_NAME) +
             ", localBindAddress=" +
             this.params.get(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME) +
             ", name=" +
             name +
             ", refreshTimeout=" +
             this.params.get(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME) +
             "]";
   }
   
   
}
