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
package org.hornetq.tests.unit.core.buffers;

import org.hornetq.core.buffers.HornetQChannelBuffer;
import org.hornetq.core.buffers.HornetQChannelBuffers;

/**
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 * Stripped down by Clebert Suconic for HornetQ
 *
 * @version $Rev: 237 $, $Date: 2008-09-04 06:53:44 -0500 (Thu, 04 Sep 2008) $
 */
public class HeapChannelBufferTest extends ChannelBuffersTestBase
{

   @Override
   protected HornetQChannelBuffer newBuffer(final int length)
   {
      HornetQChannelBuffer buffer = HornetQChannelBuffers.buffer(length);
      assertEquals(0, buffer.writerIndex());
      return buffer;
   }

   public void testShouldNotAllowNullInConstructor()
   {
      try
      {
         HornetQChannelBuffers.wrappedBuffer((byte[])null);
         fail("Exception expected");
      }
      catch (NullPointerException e)
      {

      }
   }
}
