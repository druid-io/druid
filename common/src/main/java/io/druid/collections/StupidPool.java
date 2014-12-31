/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.collections;

import com.google.common.base.Supplier;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The Stupid Pool series of ResourcePool implementations simply maintain a Queue of objects which is
 * populated upon ResourceHolder closure.
 */
public class StupidPool<T> implements ResourcePool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final Supplier<T> generator;

  protected final Queue<T> objects = new ConcurrentLinkedQueue<T>();

  public StupidPool(
      Supplier<T> generator
  )
  {
    this.generator = generator;
  }

  public final ResourceHolder<T> take()
  {
    T object = objects.poll();
    if (null == object) {
      object = generator.get();
    }
    final T innerOjbect = object;
    return new ResourceHolder<T>()
    {
      private boolean closed = false;
      private final T object = innerOjbect;

      @Override
      public synchronized T get()
      {
        if (closed) {
          throw new ISE("Already Closed!");
        }

        return object;
      }

      @Override
      public synchronized void close() throws IOException
      {
        if (closed) {
          log.warn(new ISE("Already Closed!"), "Already closed");
          return;
        }

        closed = true;
        objects.offer(object);
      }

      @Override
      protected void finalize() throws Throwable
      {
        if (!closed) {
          log.warn("Not closed!  Object was[%s]. Allowing gc to prevent leak.", object);
        }
      }
    };
  }
}
