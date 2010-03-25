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
package org.hornetq.jms.persistence.impl;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.utils.BufferHelper;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Mar 25, 2010
 */
public class PersistedJNDIBinding implements EncodingSupport
{
   private long id;

   private DestinationType type;

   private String name;

   private String jndiBinding;

   public PersistedJNDIBinding()
   {
   }

   public PersistedJNDIBinding(final DestinationType type, final String name, final String jndiBinding)
   {
      this.type = type;
      this.name = name;
      this.jndiBinding = jndiBinding;
   }

      public long getId()
   {
      return id;
   }

   public void setId(final long id)
   {
      this.id = id;
   }

   public String getName()
   {
      return name;
   }

   public String getJndiBinding()
   {
      return jndiBinding;
   }

   public DestinationType getType()
   {
      return type;
   }
   
   public int getEncodeSize()
   {
      return DataConstants.SIZE_INT +
            BufferHelper.sizeOfSimpleString(name) +
            BufferHelper.sizeOfSimpleString(jndiBinding);
   }

   public void encode(final HornetQBuffer buffer)
   {
      buffer.writeInt(type.getType());
      buffer.writeString(name);
      buffer.writeString(jndiBinding);
   }

   public void decode(final HornetQBuffer buffer)
   {
      type = DestinationType.getType(buffer.readInt());
      name = buffer.readString();
      jndiBinding = buffer.readString();
   }
}
