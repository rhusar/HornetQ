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
package org.hornetq.core.server.group.impl;

import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class GroupingHandlerConfiguration
{
   private final SimpleString name;

   private final TYPE type;

   private final SimpleString address;

   public GroupingHandlerConfiguration(final SimpleString name, final TYPE type, SimpleString address)
   {
      this.type = type;
      this.name = name;
      this.address = address;
   }

   public SimpleString getName()
   {
      return name;
   }

   public TYPE getType()
   {
      return type;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public enum TYPE
   {
      LOCAL("LOCAL"),
      REMOTE("REMOTE");

      private String type;

      TYPE(String type)
      {
         this.type = type;
      }

      public String getType()
      {
         return type;
      }
   }
}
