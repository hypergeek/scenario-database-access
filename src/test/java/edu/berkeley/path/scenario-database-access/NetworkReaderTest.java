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

/**
 * Tests methods for reading Networks from a database.
 * @author vjoel
 */
public class NetworkReaderTest {
  static NetworkReader nwReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    nwReader = new NetworkReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume network 99999 exists, but we could insert it here
    // we assume network 99990 exists
  }
  
  @Test
  public void testReadOneNetwork() throws core.DatabaseException {
    Long networkID = 99999L;
    Network nw;
        
    nw = nwReader.read(networkID);

    //System.out.println("Test Network: " + nw);
    
    assertEquals(networkID, nw.getLongId());

    assertEquals(2, nw.getNodeList().size());
  }
  
  @Test
  public void test_resolveReferences() throws core.DatabaseException {
    Long networkID = 99990L;
    Network nw;
        
    nw = nwReader.read(networkID);

    //System.out.println("Test Network: " + nw);
    
    assertEquals(networkID, nw.getLongId());

    assertEquals(3, nw.getNodeList().size());
    assertEquals(2, nw.getLinkList().size());
    
    Link ln1 = nw.getLinkById(1L);
    assert(null != ln1);
    
    Node ln1Begin = ln1.getBegin();
    assert(null != ln1Begin);
    
    Node ln1End = ln1.getEnd();
    assert(null != ln1End);
    
    Node nd1 = nw.getNodeById(1L);
    assertEquals(ln1Begin, nd1);
    
    Node nd2 = nw.getNodeById(2L);
    assertEquals(ln1End, nd2);
  }
}
