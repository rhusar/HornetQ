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
public class Proposal
{
   private final SimpleString groupId;
   private final SimpleString clusterName;

   public static final String PROPOSAL_TYPE_HEADER = "_JBM_PROPOSAL_TYPE";
   public static final String PROPOSAL_HEADER = "_JBM_PROPOSAL";

   public Proposal(SimpleString groupId, SimpleString clusterName)
   {
      this.clusterName = clusterName;
      this.groupId = groupId;
   }

   public SimpleString getGroupId()
   {
      return groupId;
   }

   public SimpleString getClusterName()
   {
      return clusterName;
   }
}

