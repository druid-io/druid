/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord;

import com.google.common.collect.Sets;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.List;
import java.util.Set;

public class PortFinder
{
  private final Set<Integer> usedPorts = Sets.newHashSet();
  private final int startPort;
  private final List<Integer> candidatePorts;

  public PortFinder(int startPort, List<Integer> candidatePorts)
  {
    this.startPort = startPort;
    this.candidatePorts = candidatePorts;
  }

  private static boolean canBind(int portNum)
  {
    try {
      new ServerSocket(portNum).close();
      return true;
    }
    catch (SocketException se) {
      return false;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized int findUnusedPort()
  {
    if (candidatePorts != null && !candidatePorts.isEmpty()) {
      int port = chooseFromCandidates();
      usedPorts.add(port);
      return port;
    } else {
      int port = chooseNext(startPort);
      while (!canBind(port)) {
        port = chooseNext(port + 1);
      }
      usedPorts.add(port);
      return port;
    }
  }

  public synchronized Pair<Integer, Integer> findTwoUnusedPorts()
  {
    if (candidatePorts != null && !candidatePorts.isEmpty()) {
      int firstPort = chooseFromCandidates();
      int secondPort = chooseFromCandidates();
      usedPorts.add(firstPort);
      usedPorts.add(secondPort);
      return new Pair<>(firstPort, secondPort);
    } else {
      int firstPort = chooseNext(startPort);
      while (!canBind(firstPort) || !canBind(firstPort + 1)) {
        firstPort = chooseNext(firstPort + 1);
      }
      usedPorts.add(firstPort);
      usedPorts.add(firstPort + 1);
      return new Pair<>(firstPort, firstPort + 1);
    }
  }

  public synchronized void markPortUnused(int port)
  {
    usedPorts.remove(port);
  }

  private int chooseFromCandidates()
  {
    for (int port : candidatePorts) {
      if (!usedPorts.contains(port) && canBind(port)) {
        return port;
      }
    }
    throw new ISE("All ports are used...");
  }

  private int chooseNext(int start)
  {
    // up to unsigned short max (65535)
    for (int i = start; i <= 0xFFFF; i++) {
      if (!usedPorts.contains(i)) {
        return i;
      }
    }
    throw new ISE("All ports are used...");
  }
}

