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
 * Tests methods for writing Nodes to a database.
 * @author vjoel
 */
public class NodeWriterTest {
  static NodeWriter ndWriter;
  static NodeReader ndReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    DBParams dbParams = new DBParams();

    ndWriter = new NodeWriter(dbParams);
    ndReader = new NodeReader(dbParams);
  }

  @Before
  public void setup() throws core.DatabaseException {
    // we assume node (1, 99995) exists, but we could insert it here
    // assume node_names has rows for (1, 99995)
    // we assume network 99996 exists, but we could insert it here
    
    ndWriter.deleteAllNodes(99996L);
  }
  
  @Test
  public void testUpdateOneNode() throws core.DatabaseException {
    Long networkID = 99995L;
    
    Node nd = new Node();
    nd.setId(1L);
    nd.setName("alice");
        
    ndWriter.update(nd, networkID);
    
    Node nd2 = ndReader.read(nd.getLongId(), networkID);
    
    assertTrue(null != nd2);
    assertEquals(nd.getLongId(), nd2.getLongId());
    assertEquals("alice", nd2.getName());

    nd.setName("bob");

    ndWriter.update(nd, networkID);
    
    Node nd3 = ndReader.read(nd2.getLongId(), networkID);
    
    assertTrue(null != nd3);
    assertEquals(nd2.getLongId(), nd3.getLongId());
    assertEquals("bob", nd3.getName());
  }
  
  @Test
  public void testInsertDeleteOneNode() throws core.DatabaseException {
    Long networkID = 99995L;

    Node nd = new Node();
    nd.setId(2L);
    
    ndWriter.insert(nd, networkID);
        
    Node nd2 = ndReader.read(nd.getLongId(), networkID);

    assertTrue(null != nd2);
    assertEquals(nd.getLongId(), nd2.getLongId());
    
    ndWriter.delete(nd.getLongId(), networkID);
    
    Node nd3 = ndReader.read(nd.getLongId(), networkID);
    assertEquals(null, nd3);
  }

  @Test
  public void testInsertDeleteAllNodesInNetwork() throws core.DatabaseException {
    Long networkID = 99996L;

    Long nd1Id = 1L;
    Long nd2Id = 2L;
    
    Node nd1 = new Node();
    nd1.setId(nd1Id);
    Node nd2 = new Node();
    nd2.setId(nd2Id);

    ArrayList<Node> nodes = new ArrayList<Node>();
    
    nodes.add(nd1);
    nodes.add(nd2);
    
    // NOTE: no transaction in the following
    ndWriter.insertNodes(nodes, networkID);

    //System.out.println("testWriteAllNodesInNetwork: get(0): " + nodes.get(0));
    //System.out.println("testWriteAllNodesInNetwork: get(1): " + nodes.get(1));
    
    ArrayList<Node> nodes2 = ndReader.readNodes(networkID);
    
    assertEquals(2, nodes2.size());
    
    HashSet<Long> expectedIds = new HashSet<Long>();
    HashSet<Long> actualIds = new HashSet<Long>();

    expectedIds.add(nd1Id);
    expectedIds.add(nd2Id);
    
    actualIds.add(nodes2.get(0).getLongId());
    actualIds.add(nodes2.get(1).getLongId());
    
    assertEquals(expectedIds, actualIds);
  }
}
