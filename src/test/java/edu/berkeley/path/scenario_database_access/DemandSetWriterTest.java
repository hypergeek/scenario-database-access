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
 * Tests methods for writing DemandSets to a database.
 * @author vjoel
 */
public class DemandSetWriterTest {
  static DemandSetReader dsReader;
  static DemandSetWriter dsWriter;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    dsReader = new DemandSetReader(new DBParams());
    dsWriter = new DemandSetWriter();
  }

  @Before
  public void setup() {
    // we assume demand set 99999 exists, but we could insert it here
  }
  
  @Test
  public void testInsertRow() throws core.DatabaseException {
    DemandSet ds = new DemandSet();
    ds.setName("writer-test");
    ds.setDescription("for test");

    //System.out.println("Test Demand -- input: " + ds);

    Long demandSetID = dsWriter.insert(ds);
    
    DemandSet ds2 = dsReader.read(demandSetID);
    assertTrue(null != ds2);

    //System.out.println("Test Demand -- output: " + ds2);
    
    assertEquals(demandSetID.toString(), ds2.getId());
    
    dsWriter.delete(demandSetID);
    
    DemandSet ds3 = dsReader.read(demandSetID);
    assertEquals(null, ds3);
  }
  
  @Test
  public void testInsertDeleteDemandSet() throws core.DatabaseException {
    DemandSet ds = new DemandSet();
    ds.setName("writer-test");
    ds.setDescription("for test");
    Map<String,DemandProfile> profMap = ds.getProfileMap();
    
    DemandProfile prof = new DemandProfile();

    prof.setStartTime(25200.0);
    prof.setSampleRate(600.0);
    prof.setKnob(1.5);
    prof.setStdDevAdd(100.0);
    prof.setStdDevMult(2.0);
    prof.setDestinationNetworkLongId(99999L);
    
    prof.addFlowAt(1L, 0.5);
    prof.addFlowAt(1L, 0.2);
    prof.addFlowAt(1L, 0.6);
    
    profMap.put("2", prof);
    
    //System.out.println("Test Demand -- input: " + ds);
    // {"id": "99998", "name": "writer-test", "description": "for test", "profile": {"2": {"destinationNetworkId": null, "startTime": 25200.0, "sampleRate": 600.0, "knob": 1.5, "stdDevAdd": 100.0, "stdDevMult": 2.0, "flow": {"1": [0.5, 0.2, 0.6]}}}}

    Long demandSetID = dsWriter.insert(ds);
    
    DemandSet ds2 = dsReader.read(demandSetID);
    assertTrue(null != ds2);

    //System.out.println("Test Demand -- output: " + ds2);
    // {"id": "99998", "name": "writer-test", "description": "for test", "profile": {"2": {"destinationNetworkId": null, "startTime": 25200.0, "sampleRate": 600.0, "knob": 1.5, "stdDevAdd": 100.0, "stdDevMult": 2.0, "flow": {"1": [0.5, 0.2, 0.6]}}}}
    
    assertEquals(demandSetID.toString(), ds2.getId());
    
    DemandProfile dp = ds2.getProfileMap().get("2");
    
    assertTrue(null != dp);
    
    assertEquals((Double)25200.0, dp.getStartTime());
    assertEquals((Double)600.0, dp.getSampleRate());
    assertEquals((Double)1.5, dp.getKnob());
    assertEquals((Double)100.0, dp.getStdDevAdd());
    assertEquals((Double)2.0, dp.getStdDevMult());
    
    assertEquals((Double)0.2, dp.getFlow().get("1").get(1));
    
    dsWriter.delete(demandSetID);
    
    DemandSet ds3 = dsReader.read(demandSetID);
    assertEquals(null, ds3);
  }

  @Test
  public void testUpdateRightModstamp() throws core.DatabaseException {
    Long demandSetID = 99999L;
    DemandSet ds =  dsReader.read(demandSetID);
    assertTrue(null != ds);
    
    try {
      dsWriter.update(ds); // should not throw
    }
    catch (ConcurrencyException exc) {
      fail("unexpected ConcurrencyException");
    }    
  }

  @Test
  public void testUpdateWrongModstamp() throws core.DatabaseException {
    Long demandSetID = 99999L;
    DemandSet ds =  dsReader.read(demandSetID);
    assertTrue(null != ds);
    
    //System.out.println("ds = " + ds);
    ds.setModstamp("29-JAN-2000 09-48-12.465014");
    
    try {
      dsWriter.update(ds); // should throw
      fail("exception was expected but not thrown");
    }
    catch(ConcurrencyException exc) {
      // as expected
    }
  }
}
