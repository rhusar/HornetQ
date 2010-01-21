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

package org.hornetq.integration.transports.netty;

import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.server.HornetQServer;

/**
 * A ServerHolder
 *
 * @author jmesnil
 *
 *
 */
public interface ServerHolder
{
   HornetQServer getServer();

   // FIXME should NOT be CoreRemotingConnection but RemotingConnection
   CoreRemotingConnection getRemotingConnection(int connectionID);
}
