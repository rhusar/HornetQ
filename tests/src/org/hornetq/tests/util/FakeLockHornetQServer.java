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

package org.hornetq.tests.util;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.cluster.LockFile;
import org.hornetq.core.server.cluster.impl.FakeLockFile;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.spi.core.security.HornetQSecurityManager;

import javax.management.MBeanServer;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 23, 2010
 */
public class FakeLockHornetQServer extends HornetQServerImpl
{
   public FakeLockHornetQServer()
   {
      super();    //To change body of overridden methods use File | Settings | File Templates.
   }

   public FakeLockHornetQServer(Configuration configuration)
   {
      super(configuration);    //To change body of overridden methods use File | Settings | File Templates.
   }

   public FakeLockHornetQServer(Configuration configuration, MBeanServer mbeanServer)
   {
      super(configuration, mbeanServer);    //To change body of overridden methods use File | Settings | File Templates.
   }

   public FakeLockHornetQServer(Configuration configuration, HornetQSecurityManager securityManager)
   {
      super(configuration, securityManager);    //To change body of overridden methods use File | Settings | File Templates.
   }

   public FakeLockHornetQServer(Configuration configuration, MBeanServer mbeanServer, HornetQSecurityManager securityManager)
   {
      super(configuration, mbeanServer, securityManager);    //To change body of overridden methods use File | Settings | File Templates.
   }

   @Override
   protected LockFile createLockFile(String fileName, String directory)
   {
      return new FakeLockFile(fileName, directory);
   }
}
