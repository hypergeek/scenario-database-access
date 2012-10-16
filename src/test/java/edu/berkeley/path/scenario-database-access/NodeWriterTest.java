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

import edu.berkeley.path.model_elements.*;

/**
 * Tests methods for writing Nodes to a database.
 * @author vjoel
 */
public class NodeWriterTest {
  static NodeWriter ndWriter;
  static NodeReader ndReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    DBParams dpParams = new DBParams();

    ndWriter = new NodeWriter(dpParams);
    ndReader = new NodeReader(dpParams);
  }

  @Before
  public void setup() {
    // we assume node (1, 99998) exists, but we could insert it here
    // we assume node (1, 99997) does not exist, but we could delete it here
  }
  
  @Test
  public void testWriteOneNode() throws core.DatabaseException {
    Long networkID = 99998L;
    
    Node nd = new Node();
    nd.setId(1L);
        
    ndWriter.update(nd, networkID);
    
    Node nd2 = ndReader.read(nd.getLongId(), networkID);
    
    assertTrue(null != nd2);
    assertEquals(nd.getLongId(), nd2.getLongId());
  }
  
  @Test
  public void testInsertDeleteOneNetwork() throws core.DatabaseException {
    Long networkID = 99997L;

    Node nd = new Node();
    nd.setId(1L);
    
    ndWriter.insert(nd, networkID);
        
    Node nd2 = ndReader.read(nd.getLongId(), networkID);

    assertTrue(null != nd2);
    assertEquals(nd.getLongId(), nd2.getLongId());
    
    ndWriter.delete(nd.getLongId(), networkID);
    
    Node nd3 = ndReader.read(nd.getLongId(), networkID);
    assertEquals(null, nd3);
  }
}
