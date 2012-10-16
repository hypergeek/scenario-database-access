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
 * Tests methods for reading Links from a database.
 * @author vjoel
 */
public class LinkReaderTest {
  static LinkReader lnReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    lnReader = new LinkReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume links (1, 99993) and (2, 99993) exist, but we could insert them here
  }
  
  @Test
  public void testReadOneLink() throws core.DatabaseException {
    Long networkID = 99993L;
    Long ln1Id = 1L;
    Long ln2Id = 2L;
    Link ln1;
    Link ln2;
    
    ln1 = lnReader.read(1L, networkID);
    ln2 = lnReader.read(2L, networkID);

    //System.out.println("testReadOneLink: ln1: " + ln1);
    //System.out.println("testReadOneLink: ln2: " + ln2);
    
    assertEquals(ln1Id, ln1.getLongId());
    assertEquals(ln2Id, ln2.getLongId());
    
    assertEquals("1", ln1.getBeginId());
    assertEquals("2", ln1.getEndId());
    
    assertEquals("2", ln2.getBeginId());
    assertEquals("3", ln2.getEndId());

//TODO
//    assertEquals(123, ln1.getSpeedLimit());
//    assertEquals(124, ln2.getSpeedLimit());

    assertEquals((Double)1000.0, ln1.getLength());
    assertEquals((Double)1001.0, ln2.getLength());

//TODO
//    assertEquals(1, ln1.getDetailLevel());
//    assertEquals(2, ln2.getDetailLevel());
  }
  
  @Test
  public void testReadAllLinksInNetwork() throws core.DatabaseException {
    Long networkID = 99993L;
    Long ln1Id = 1L;
    Long ln2Id = 2L;
    
    // NOTE: no transaction in the following
    ArrayList<Link> links = lnReader.readLinks(networkID);

    //System.out.println("testReadAllLinksInNetwork: get(0): " + links.get(0));
    //System.out.println("testReadAllLinksInNetwork: get(1): " + links.get(1));
    
    assertEquals(2, links.size());
    
    HashSet<Long> expectedIds = new HashSet<Long>();
    HashSet<Long> actualIds = new HashSet<Long>();
    
    expectedIds.add(ln1Id);
    expectedIds.add(ln2Id);
    
    actualIds.add(links.get(0).getLongId());
    actualIds.add(links.get(1).getLongId());
    
    assertEquals(expectedIds, actualIds);
  }
}
