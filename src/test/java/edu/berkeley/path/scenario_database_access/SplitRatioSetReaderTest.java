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
 * Tests methods for reading SplitRatioSets from a database.
 * @author vjoel
 */
public class SplitRatioSetReaderTest {
  static SplitRatioSetReader srsReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    srsReader = new SplitRatioSetReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume splitratio set 99999 exists, but we could insert it here
  }
  
  @Test
  public void testReadSplitRatioSet() throws core.DatabaseException {
    Long splitratioSetID = 99999L;
    SplitRatioSet srs;
        
    srs = srsReader.read(splitratioSetID);

    //System.out.println("Test SplitRatio: " + srs);
    // {"id": "99999", "name": "scenario-database-access-test", "description": null, "projectId": "1", "profile": {"2": {"destinationNetworkId": null, "startTime": 25200.0, "sampleRate": 600.0, "ratio": {"1": {"1": {"1": [0.5, 0.2, 0.6]}}}}}}
    
    assertEquals(splitratioSetID.toString(), srs.getId());
    
    SplitRatioProfile srp = srs.getProfileMap().get("2");
    
    assertTrue(null != srp);
    
    assertEquals((Double)25200.0, srp.getStartTime());
    assertEquals((Double)600.0, srp.getSampleRate());
    
    assertEquals((Double)0.2, srp.getRatio().get("1").get("1").get("1").get(1));
  }
}
