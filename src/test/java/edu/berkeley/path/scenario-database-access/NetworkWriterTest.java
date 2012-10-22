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

import edu.berkeley.path.model_elements.*;

import java.util.ArrayList;

/**
 * Tests methods for writing Networks to a database.
 * @author vjoel
 */
public class NetworkWriterTest {
  static NetworkWriter nwWriter;
  static NetworkReader nwReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    DBParams dbParams = new DBParams();
    
    nwWriter = new NetworkWriter(dbParams);
    nwReader = new NetworkReader(dbParams);
  }

  @Before
  public void setup() {
    // we assume network 99998 exists, but we could insert it here
    // we assume network 99997 does not exist, but we could delete it here
  }
  
  @Test
  public void testUpdateOneNetwork() throws core.DatabaseException {
    Long networkID = 99998L;
    String name = "NetworkWriterTest testUpdateOneNetwork";
    String desc = "for test";
    
    Network nw = new Network();
    
    nw.setId(networkID);
    nw.setName(name);
    nw.setDescription(desc);
    
    //System.out.println("Test Network: " + nw);
    
    nwWriter.update(nw);
    
    Network nw2 = nwReader.read(nw.getLongId());
    
    assertEquals(networkID, nw2.getLongId());
    assertEquals(name, nw2.getName());
    assertEquals(desc, nw2.getDescription());
  }

  @Test
  public void testInsertDeleteOneNetwork() throws core.DatabaseException {
    Long networkID = 99997L;
    String name = "NetworkWriterTest testInsertDeleteOneNetwork";
    String desc = "for test";

    Network nw = new Network();
    
    nw.setId(networkID);
    nw.setName(name);
    nw.setDescription(desc);
    
    //System.out.println("Test Network: " + nw);
    
    nwWriter.insert(nw);
    
    Network nw2 = nwReader.read(nw.getLongId());

    assertTrue(null != nw2);
    assertEquals(networkID, nw2.getLongId());
    
    nwWriter.delete(networkID);
    
    Network nw3 = nwReader.read(networkID);
    assertEquals(null, nw3);
  }

  @Test
  public void testUpdateOneNetworkWithNodes() throws core.DatabaseException {
    // TODO
  }

  @Test
  public void testInsertDeleteOneNetworkWithNodes() throws core.DatabaseException {
    Long networkID = 99997L;
    String name = "NetworkWriterTest testInsertDeleteOneNetworkWithNodes";
    String desc = "for test";

    Network nw = new Network();
    
    nw.setId(networkID);
    nw.setName(name);
    nw.setDescription(desc);
    
    nw.setNodeList(new ArrayList<Node>());
    
    Node nd = new Node();
    nd.setId(42L);
    
    nw.getNodes().add(nd);

    //System.out.println("Test Network: " + nw);
    
    nwWriter.insert(nw);
    
    Network nw2 = nwReader.read(nw.getLongId());

    assertTrue(null != nw2);
    assertEquals(1, nw2.getNodes().size());
    assertEquals(nd.getLongId(), ((Node)nw2.getNodes().get(0)).getLongId());
    
    nwWriter.delete(networkID);
    
    Network nw3 = nwReader.read(networkID);
    assertEquals(null, nw3);
    assertEquals(0, (new NodeReader(new DBParams())).readNodes(networkID).size());
  }

  @Test
  public void testInsertDeleteOneNetworkWithNodesAndLinks() throws core.DatabaseException {
    Long networkID = 99997L;
    String name = "NetworkWriterTest testInsertDeleteOneNetworkWithNodesAndLinks";
    String desc = "for test";

    Network nw = new Network();
    
    nw.setId(networkID);
    nw.setName(name);
    nw.setDescription(desc);
    
    nw.setNodeList(new ArrayList<Node>());
    nw.setLinkList(new ArrayList<Link>());
    
    Node nd1 = new Node();
    nd1.setId(42L);
    nw.getNodes().add(nd1);

    Node nd2 = new Node();
    nd2.setId(43L);
    nw.getNodes().add(nd2);
    
    Link ln1 = new Link();
    ln1.setId(44L);
    ln1.setBegin(nd1);
    ln1.setEnd(nd2);
    nw.getLinks().add(ln1);

    //System.out.println("Test Network: " + nw);
    
    nwWriter.insert(nw);
    
    Network nw2 = nwReader.read(nw.getLongId());

    assertTrue(null != nw2);

    assertEquals(2, nw2.getNodes().size());
    assertEquals(1, nw2.getLinks().size());
    Link ln2 = (Link)nw2.getLinks().get(0);
    assertEquals(ln1.getLongId(), ln2.getLongId());
    
    nwWriter.delete(networkID);
    
    Network nw3 = nwReader.read(networkID);
    assertEquals(null, nw3);
    assertEquals(0, (new NodeReader(new DBParams())).readNodes(networkID).size());
    assertEquals(0, (new LinkReader(new DBParams())).readLinks(networkID).size());
  }
}
