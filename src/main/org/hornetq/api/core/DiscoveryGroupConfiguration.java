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

package org.hornetq.api.core;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
/**
 * A DiscoveryGroupConfiguration
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class DiscoveryGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 2877108926493109407L;

   private final String serverLocatorClassName;
   
   private final String name; 
   
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
   
}
