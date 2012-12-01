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

import java.util.*;

/**
 * Tests methods for writing SplitRatioSets to a database.
 * @author vjoel
 */
public class SplitRatioSetWriterTest {
  static SplitRatioSetReader srsReader;
  static SplitRatioSetWriter srsWriter;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    srsReader = new SplitRatioSetReader(new DBParams());
    srsWriter = new SplitRatioSetWriter(new DBParams());
  }

  @Before
  public void setup() {
    // we assume splitratio set 99998 does not exist, but we could delete it here
  }
  
  @Test
  public void testInsertDeleteSplitRatioSet() throws core.DatabaseException {
    Long splitratioSetID = 99998L;
    SplitRatioSet srs = new SplitRatioSet();
    srs.setId(splitratioSetID);
    srs.setName("writer-test");
    srs.setDescription("for test");
    Map<String,SplitRatioProfile> profMap = srs.getProfileMap();
    
    SplitRatioProfile prof = new SplitRatioProfile();

    prof.setStartTime(25200.0);
    prof.setSampleRate(600.0);
    //prof.setDestinationNetworkLongId(99999L);
    // leave this null for now
    
    prof.addRatioAt(1L, 1L, 1L, 0.5);
    prof.addRatioAt(1L, 1L, 1L, 0.2);
    prof.addRatioAt(1L, 1L, 1L, 0.6);
    
    profMap.put("2", prof);
    
    System.out.println("Test SplitRatio -- input: " + srs);
    // {"id": "99998", "name": "writer-test", "description": null, "profile": {"2": {"destinationNetworkId": "1", "startTime": 25200.0, "sampleRate": 600.0, "ratio": {"1": {"1": {"1": [0.5, 0.2, 0.6]}}}}}}

    srsWriter.insert(srs);
    
    SplitRatioSet srs2 = srsReader.read(splitratioSetID);
    assertTrue(null != srs2);

    System.out.println("Test SplitRatio -- output: " + srs2);
    // {"id": "99998", "name": "writer-test", "description": null, "profile": {"2": {"destinationNetworkId": "1", "startTime": 25200.0, "sampleRate": 600.0, "ratio": {"1": {"1": {"1": [0.5, 0.2, 0.6]}}}}}}
    
    assertEquals(splitratioSetID.toString(), srs2.getId());
    
    SplitRatioProfile srp = srs2.getProfileMap().get("2");
    
    assertTrue(null != srp);
    
    assertEquals((Double)25200.0, srp.getStartTime());
    assertEquals((Double)600.0, srp.getSampleRate());
    
    assertEquals((Double)0.2, srp.getRatio().get("1").get("1").get("1").get(1));
    
    srsWriter.delete(splitratioSetID);
    
    SplitRatioSet srs3 = srsReader.read(splitratioSetID);
    assertEquals(null, srs3);
  }
}
