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

package org.hornetq.utils;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * A ClassloadingUtil
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public final class ClassloadingUtil
{
   public static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            ClassLoader loader = ClassloadingUtil.class.getClassLoader();
            if (loader == null)
            {
               loader = Thread.currentThread().getContextClassLoader();
            }

            try
            {
               Class<?> clazz = loader.loadClass(className);
               return clazz.newInstance();
            }
            catch (Exception e)
            {
               throw new IllegalArgumentException("Error instantiating connector factory \"" + className + "\"", e);
            }
         }
      });
   }

}
