/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.core.config;

import org.hornetq.api.core.TransportConfiguration;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: andy
 * Date: Sep 2, 2010
 * Time: 11:36:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class BackupConnectorConfiguration implements Serializable
{
   private final List<String> staticConnectors;

   private final String discoveryGroupName;

   private String connector;

   public BackupConnectorConfiguration(List<String> staticConnectors, String connector)
   {
      this.staticConnectors = staticConnectors;
      this.discoveryGroupName = null;
      this.connector = connector;
   }

   public List<String> getStaticConnectors()
   {
      return staticConnectors;
   }

   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

   public BackupConnectorConfiguration(String discoveryGroupName, String connector)
   {
      this.staticConnectors = null;
      this.discoveryGroupName = discoveryGroupName;
      this.connector = connector;
   }

   public String getConnector()
   {
      return connector;
   }
}
