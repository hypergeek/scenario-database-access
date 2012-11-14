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
 * Tests methods for writing Links to a database.
 * @author vjoel
 */
public class LinkWriterTest {
  static LinkWriter lnWriter;
  static LinkReader lnReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    DBParams dbParams = new DBParams();

    lnWriter = new LinkWriter(dbParams);
    lnReader = new LinkReader(dbParams);
  }

  @Before
  public void setup() throws core.DatabaseException {
    // we assume link (1, 99991) exists, but we could insert it here
    // we assume network 99992 exists, but we could insert it here
    
    lnWriter.deleteAllLinks(99992L);
  }
  
  @Test
  public void testUpdateOneLink() throws core.DatabaseException {
    Long networkID = 99991L;
    
    Link ln = new Link();
    ln.setId(1L);
    ln.setLength(555.0);
    ln.setBeginId("1"); // TODO create nodes instead
    ln.setEndId("2");
    ln.setSpeedLimit(45);
    ln.setDetailLevel(2);
    ln.setName(null);
    ln.setType(null);
    ln.setLaneCount(null);
    ln.setLaneOffset(null);

    lnWriter.update(ln, networkID);
    
    Link ln2 = lnReader.read(ln.getLongId(), networkID);
    
    assertTrue(null != ln2);
    assertEquals(ln.getLongId(), ln2.getLongId());
    assertEquals((Double)555.0, ln2.getLength());
    assertEquals((Integer)45, ln2.getSpeedLimit());
    assertEquals((Integer)2, ln2.getDetailLevel());
    assertEquals(null, ln2.getName());
    assertEquals(null, ln2.getType());
    assertEquals(null, ln2.getLaneCount());
    assertEquals(null, ln2.getLaneOffset());

    ln.setName("bob"); // code path 1: update null to non-null
    ln.setType("Freeway");
    ln.setLaneCount(1.0);
    ln.setLaneOffset(2);
    lnWriter.update(ln, networkID);
    ln2 = lnReader.read(ln.getLongId(), networkID);
    assertTrue(null != ln2);
    assertEquals(ln.getLongId(), ln2.getLongId());
    assertEquals("bob", ln2.getName());
    assertEquals("Freeway", ln2.getType());
    assertEquals((Double)1.0, ln2.getLaneCount());
    assertEquals((Integer)2, ln2.getLaneOffset());

    ln.setName("alice"); // code path 2: update non-null to non-null
    ln.setType("Street");
    ln.setLaneCount(2.0);
    ln.setLaneOffset(5);
    lnWriter.update(ln, networkID);
    ln2 = lnReader.read(ln.getLongId(), networkID);
    assertTrue(null != ln2);
    assertEquals("alice", ln2.getName());
    assertEquals("Street", ln2.getType());
    assertEquals((Double)2.0, ln2.getLaneCount());
    assertEquals((Integer)5, ln2.getLaneOffset());

    ln.setName(null); // code path 3: update non-null to null
    ln.setType(null);
    ln.setLaneCount(null);
    ln.setLaneOffset(null);
    lnWriter.update(ln, networkID);
    ln2 = lnReader.read(ln.getLongId(), networkID);
    assertTrue(null != ln2);
    assertEquals(null, ln2.getName());
    assertEquals(null, ln2.getType());
    assertEquals(null, ln2.getLaneCount());
    assertEquals(null, ln2.getLaneOffset());
  }
  
  @Test
  public void testInsertDeleteOneLink() throws core.DatabaseException {
    Long networkID = 99991L;

    Link ln = new Link();
    ln.setId(2L);
    ln.setLength(555.0);
    ln.setBeginId("1"); // TODO create nodes instead
    ln.setEndId("2");
    ln.setSpeedLimit(45);
    ln.setDetailLevel(2);
    ln.setName("alice");
    ln.setType("Street");
    ln.setLaneCount(2.0);
    ln.setLaneOffset(5);
    
    lnWriter.insert(ln, networkID);
        
    Link ln2 = lnReader.read(ln.getLongId(), networkID);

    assertTrue(null != ln2);
    assertEquals(ln.getLongId(), ln2.getLongId());
    assertEquals((Double)555.0, ln2.getLength());
    assertEquals((Integer)45, ln2.getSpeedLimit());
    assertEquals((Integer)2, ln2.getDetailLevel());
    assertEquals("alice", ln2.getName());
    assertEquals("Street", ln2.getType());
    assertEquals((Double)2.0, ln2.getLaneCount());
    assertEquals((Integer)5, ln2.getLaneOffset());
    
    lnWriter.delete(ln.getLongId(), networkID);
    
    Link ln3 = lnReader.read(ln.getLongId(), networkID);
    assertEquals(null, ln3);
  }

  @Test
  public void testInsertDeleteAllLinksInNetwork() throws core.DatabaseException {
    Long networkID = 99992L;

    Long ln1Id = 1L;
    Long ln2Id = 2L;
    
    Link ln1 = new Link();
    ln1.setId(ln1Id);
    ln1.setLength(555.0);
    ln1.setBeginId("1"); // TODO create nodes instead
    ln1.setEndId("2");

    Link ln2 = new Link();
    ln2.setId(ln2Id);
    ln2.setLength(555.0);
    ln2.setBeginId("2"); // TODO create nodes instead
    ln2.setEndId("3");

    ArrayList<Link> links = new ArrayList<Link>();
    
    links.add(ln1);
    links.add(ln2);
    
    // NOTE: no transaction in the following
    lnWriter.insertLinks(links, networkID);

    //System.out.println("testWriteAllLinksInNetwork: get(0): " + links.get(0));
    //System.out.println("testWriteAllLinksInNetwork: get(1): " + links.get(1));
    
    ArrayList<Link> links2 = lnReader.readLinks(networkID);
    
    assertEquals(2, links2.size());
    
    HashSet<Long> expectedIds = new HashSet<Long>();
    HashSet<Long> actualIds = new HashSet<Long>();

    expectedIds.add(ln1Id);
    expectedIds.add(ln2Id);
    
    actualIds.add(links2.get(0).getLongId());
    actualIds.add(links2.get(1).getLongId());
    
    assertEquals(expectedIds, actualIds);
    
    HashSet<String> expectedNames = new HashSet<String>();
    HashSet<String> actualNames = new HashSet<String>();
    
    expectedNames.add(ln1.getNameString());
    expectedNames.add(ln2.getNameString());
    
    actualNames.add(links2.get(0).getNameString());
    actualNames.add(links2.get(1).getNameString());

    assertEquals(expectedNames, actualNames);
    
    lnWriter.deleteAllLinks(networkID);
    ArrayList<Link> links3 = lnReader.readLinks(networkID);
    assertEquals(0, links3.size());
    
    // todo check no rows in names or types table
  }
}
