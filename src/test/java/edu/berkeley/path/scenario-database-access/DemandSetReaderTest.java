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
 * Tests methods for reading DemandSets from a database.
 * @author vjoel
 */
public class DemandSetReaderTest {
  static DemandSetReader dsReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    dsReader = new DemandSetReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume demand set 99999 exists, but we could insert it here
  }
  
  @Test
  public void testReadDemandSet() throws core.DatabaseException {
    Long demandSetID = 99999L;
    DemandSet ds;
        
    ds = dsReader.read(demandSetID);

    //System.out.println("Test Demand: " + ds);
    // {"id": "99999", "name": "scenario-database-access-test", "description": null, "profile": {"2": {"destinationNetworkId": null, "startTime": 25200.0, "sampleRate": 600.0, "knob": 0.5, "stdDevAdd": 0.1, "stdDevMult": 0.2, "flow": {"1": [0.1, 0.2, 0.3]}}}}

    
    assertEquals(demandSetID.toString(), ds.getId());
    
    DemandProfile dp = ds.getProfileMap().get("2");
    
    assertTrue(null != dp);
    
    assertEquals((Double)25200.0, dp.getStartTime());
    assertEquals((Double)600.0, dp.getSampleRate());
    
    assertEquals((Double)0.2, dp.getFlow().get("1").get(1));
  }
}
