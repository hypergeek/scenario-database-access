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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import edu.berkeley.path.model_elements.*;

/**
 * Tests methods for reading SensorSets from a database.
 * @author vjoel
 */
public class SensorSetReaderTest {
  static SensorSetReader ssReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    ssReader = new SensorSetReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume sensor set 99999 exists, but we could insert it here
    // we assume sensor set 99998, 99999 are in the set
  }
  
  @Test
  public void testReadSensorSet() throws core.DatabaseException {
    Long sensorSetID = 99999L;
    SensorSet ss;

    ss = ssReader.read(sensorSetID);

    //System.out.println("Test SensorSet: " + ss);
    //{"id": "99999", "name": "test", "description": null, "projectId": "99999", "sensors": [{"type": "Loop", "entityId": "ent1", "measurementFeedId": "1", "linkId": "100", "linkOffset": 50.0, "laneNum": 3.0, "healthStatus": 1.0}, {"type": "Magnetic", "entityId": "ent2", "measurementFeedId": "2", "linkId": "101", "linkOffset": 60.0, "laneNum": 4.0, "healthStatus": 1.0}]}

    assertEquals(sensorSetID, ss.getLongId());
    List<Sensor> sensors = ss.getSensorList();
    assertEquals(2, sensors.size());
    
    Sensor s0 = sensors.get(0);
    assertEquals("Loop", s0.getType());
    assertEquals((Double)50.0, s0.getLinkOffset());
    
    Sensor s1 = sensors.get(1);
    assertEquals("Magnetic", s1.getType());
    assertEquals((Double)60.0, s1.getLinkOffset());
  }
}
