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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.logging.Logger;

/**
 * A BroadcastGroupConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:44:30
 *
 */
public class BroadcastGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 1052413739064253955L;

   private static final Logger log = Logger.getLogger(BroadcastGroupConfiguration.class);

   private final String broadcastGroupClassName;
   
   private final Map<String,Object> params;
   
   private final String name;

   private final List<TransportConfiguration> connectorList;
   
   public BroadcastGroupConfiguration(final String clazz,
                                      final Map<String,Object> params,
                                      final String name,
                                      final List<TransportConfiguration> connectorList)
   {
      super();
      this.broadcastGroupClassName = clazz;
      this.params = params;
      this.name = name;
      this.connectorList = connectorList;
   }

   public String getBroadcastGroupClassName()
   {
      return this.broadcastGroupClassName;
   }

   public Map<String, Object> getParams()
   {
      return this.params;
   }

   public String getName()
   {
      return name;
   }

   public List<TransportConfiguration> getConnectorList()
   {
      return connectorList;
   }

}
