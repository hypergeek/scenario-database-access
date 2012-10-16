/**
 * Copyright (c) 2012 The Regents of the University of California.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package edu.berkeley.path.scenario_database_access;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;

import edu.berkeley.path.model_elements.*;

/**
 * Tests methods for reading Nodes from a database.
 * @author vjoel
 */
public class NodeReaderTest {
  static NodeReader ndReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    ndReader = new NodeReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume nodes (1, 99999) and (2, 99999) exist, but we could insert them here
  }
  
  @Test
  public void testReadOneNode() throws core.DatabaseException {
    Long networkID = 99999L;
    Long nd1Id = 1L;
    Long nd2Id = 2L;
    Node nd1;
    Node nd2;
    
    nd1 = ndReader.read(1L, networkID);
    nd2 = ndReader.read(2L, networkID);

    //System.out.println("testReadOneNode: nd1: " + nd1);
    //System.out.println("testReadOneNode: nd2: " + nd2);
    
    assertEquals(nd1Id, nd1.getLongId());
    assertEquals(nd2Id, nd2.getLongId());
  }
  
  @Test
  public void testReadAllNodesInNetwork() throws core.DatabaseException {
    Long networkID = 99999L;
    Long nd1Id = 1L;
    Long nd2Id = 2L;
    
    // NOTE: no transaction in the following
    ArrayList<Node> nodes = ndReader.readNodes(networkID);

    //System.out.println("testReadAllNodesInNetwork: get(0): " + nodes.get(0));
    //System.out.println("testReadAllNodesInNetwork: get(1): " + nodes.get(1));
    
    assertEquals(2, nodes.size());
    
    HashSet<Long> expectedIds = new HashSet<Long>();
    HashSet<Long> actualIds = new HashSet<Long>();
    
    expectedIds.add(nd1Id);
    expectedIds.add(nd2Id);
    
    actualIds.add(nodes.get(0).getLongId());
    actualIds.add(nodes.get(1).getLongId());
    
    assertEquals(expectedIds, actualIds);
  }
}
