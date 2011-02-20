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

package org.hornetq.core.config;

/**
 * A BroadcastGroupConstants
 *
 * @author "<a href=\"tm.igarashi@gmail.com\">Tomohisa Igarashi</a>"
 *
 *
 */
public class BroadcastGroupConstants
{
   // for simple UDP broadcast
   public static final String LOCAL_BIND_ADDRESS_NAME = "local-bind-address";
   public static final String LOCAL_BIND_PORT_NAME = "local-bind-port";
   public static final String GROUP_ADDRESS_NAME = "group-address";
   public static final String GROUP_PORT_NAME = "group-port";
   public static final String BROADCAST_PERIOD_NAME = "broadcast-period";
}
