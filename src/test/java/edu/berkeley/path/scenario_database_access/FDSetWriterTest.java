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
 * Tests methods for writing FDSets to a database.
 * @author vjoel
 */
public class FDSetWriterTest {
  static FDSetReader fdsReader;
  static FDSetWriter fdsWriter;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    fdsReader = new FDSetReader(new DBParams());
    fdsWriter = new FDSetWriter(new DBParams());
  }

  @Before
  public void setup() {
    // we assume fd set 99998 does not exist, but we could delete it here
  }
  
  @Test
  public void testInsertDeleteFDSet() throws core.DatabaseException {
    Long fdSetID = 99998L;
    FDSet fds = new FDSet();
    fds.setId(fdSetID);
    fds.setName("writer-test");
    fds.setDescription("for test");
    
    FDType fdType = new FDType();
    fdType.setId(1L);
    fds.setType(fdType);
    
    Map<String,FDProfile> profMap = fds.getProfileMap();
    
    FDProfile prof = new FDProfile();

    prof.setStartTime(25200.0);
    prof.setSampleRate(600.0);
    
    List<FD> fdList = prof.getFdList();
    FD fd;
    fd = new FD();
    fd.jamDensity = 1.23;
    fdList.add(fd);
    fd = new FD();
    fd.jamDensity = 4.56;
    fdList.add(fd);
    
    profMap.put("2", prof);
    
    //System.out.println("Test FD -- input: " + fds);
    // {"id": "99998", "name": "writer-test", "description": "for test", "type": {"id": "1", "name": null, "description": null}, "profile": {"2": {"startTime": 25200.0, "sampleRate": 600.0, "fd": [{"freeFlowSpeed": null, "criticalSpeed": null, "congestionWaveSpeed": null, "capacity": null, "jamDensity": 1.23, "capacityDrop": null, "freeFlowSpeedStd": null, "congestionWaveSpeedStd": null, "capacityStd": null}, {"freeFlowSpeed": null, "criticalSpeed": null, "congestionWaveSpeed": null, "capacity": null, "jamDensity": 4.56, "capacityDrop": null, "freeFlowSpeedStd": null, "congestionWaveSpeedStd": null, "capacityStd": null}]}}}

    fdsWriter.insert(fds);
    
    FDSet fds2 = fdsReader.read(fdSetID);
    assertTrue(null != fds2);

    //System.out.println("Test FD -- output: " + fds2);
    // {"id": "99998", "name": "writer-test", "description": "for test", "type": {"id": "1", "name": "Triangular", "description": null}, "profile": {"2": {"startTime": 25200.0, "sampleRate": 600.0, "fd": [{"freeFlowSpeed": null, "criticalSpeed": null, "congestionWaveSpeed": null, "capacity": null, "jamDensity": 1.23, "capacityDrop": null, "freeFlowSpeedStd": null, "congestionWaveSpeedStd": null, "capacityStd": null}, {"freeFlowSpeed": null, "criticalSpeed": null, "congestionWaveSpeed": null, "capacity": null, "jamDensity": 4.56, "capacityDrop": null, "freeFlowSpeedStd": null, "congestionWaveSpeedStd": null, "capacityStd": null}]}}}
    
    assertEquals(fdSetID.toString(), fds2.getId());
    
    // Note that the name is filled in from the database, even though it wasn't present when inserted.
    assertEquals("Triangular", fds2.getType().getName());
    
    FDProfile fdp = fds2.getProfileMap().get("2");
    
    assertTrue(null != fdp);
    
    assertEquals((Double)25200.0, fdp.getStartTime());
    assertEquals((Double)600.0, fdp.getSampleRate());
    
    assertEquals((Double)1.23, fdp.getFdList().get(0).getJamDensity());
    assertEquals((Double)4.56, fdp.getFdList().get(1).getJamDensity());
    
    fdsWriter.delete(fdSetID);
    
    FDSet fds3 = fdsReader.read(fdSetID);
    assertEquals(null, fds3);
  }
}
