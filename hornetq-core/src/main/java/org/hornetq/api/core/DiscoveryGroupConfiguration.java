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

   private String serverLocatorClassName;

   private String clusterConnectorClassName;

   private String name;

   private final Map<String, Object> params;

   public DiscoveryGroupConfiguration(final String serverLocatorClassName,
                                      final String clusterConnectorClassName,
                                      final Map<String, Object> params,
                                      final String name)
   {
      this.serverLocatorClassName = serverLocatorClassName;
      this.clusterConnectorClassName = clusterConnectorClassName;
      this.name = name;
      this.params = params;
   }

   public String getName()
   {
      return name;
   }

   public String getServerLocatorClassName()
   {
      return serverLocatorClassName;
   }

   public String getClusterConnectorClassName()
   {
      return clusterConnectorClassName;
   }

   public Map<String, Object> getParams()
   {
      return params;
   }

   public void setName(final String name)
   {
      this.name = name;
   }

   public void setServerLocatorClassName(String name)
   {
      this.serverLocatorClassName = name;
   }

   public void setClusterConnectorClassName(String name)
   {
      this.clusterConnectorClassName = name;
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DiscoveryGroupConfiguration that = (DiscoveryGroupConfiguration) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;
      if (serverLocatorClassName != null ? !serverLocatorClassName.equals(that.serverLocatorClassName)
                                        : that.serverLocatorClassName != null)
         return false;
      if (params == null && that.params != null)
         return false;

      if (params.keySet().size() != that.params.keySet().size())
         return false;
      for (String key : params.keySet())
      {
         if (!params.get(key).equals(that.params.get(key)))
            return false;
      }

      return true;
   }

   @Override
   public int hashCode()
   {
      int result = name != null ? name.hashCode() : 0;
      for (String key : params.keySet())
      {
         result = 31 * result + (params.get(key) != null ? params.get(key).hashCode() : 0);
      }
      return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      StringBuilder str =
               new StringBuilder().append("DiscoveryGroupConfiguration [serverLocatorClassName=")
                                  .append(serverLocatorClassName)
                                  .append(", name=")
                                  .append(name);
      for (String key : params.keySet())
      {
         str.append(", ").append(key).append("=").append(params.get(key));
      }
      return str.append("]").toString();
   }


}
