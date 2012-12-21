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
 * Tests methods for reading FDSets from a database.
 * @author vjoel
 */
public class FDSetReaderTest {
  static FDSetReader fdsReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    fdsReader = new FDSetReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume fd set 99999 exists, but we could insert it here
    // we assume fd profs 99998, 99999 exist, but we could insert here
  }
  
  @Test
  public void testReadFDSet() throws core.DatabaseException {
    Long fdSetID = 99999L;
    FDSet fds;
        
    fds = fdsReader.read(fdSetID);

    //System.out.println("Test FD: " + fds);
    
    assertEquals(fdSetID.toString(), fds.getId());
    
    FDType fdType = (FDType)fds.getType();
    assertTrue(null != fdType);
    assertEquals("Triangular", fdType.getName());
    
    FDProfile fdp100 = fds.getProfileMap().get("100");
    
    assertTrue(null != fdp100);
    
    assertEquals((Double)25200.0, fdp100.getStartTime());
    assertEquals((Double)600.0, fdp100.getSampleRate());
    
    FDProfile fdp101 = fds.getProfileMap().get("101");
    
    assertTrue(null != fdp101);
    
    assertEquals((Double)25300.0, fdp101.getStartTime());
    assertEquals((Double)700.0, fdp101.getSampleRate());
    
    assertEquals(2, fdp100.getFdList().size());
    assertEquals(2, fdp101.getFdList().size());
    
    assertEquals((Double)1.1, fdp100.getFdList().get(1).getCapacity());
    assertEquals((Double)2.0, fdp101.getFdList().get(0).getCapacity());
  }
}
