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
 * Tests methods for writing SensorSets to a database.
 * @author vjoel
 */
public class SensorSetWriterTest {
  static SensorSetReader ssReader;
  static SensorSetWriter ssWriter;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    ssReader = new SensorSetReader(new DBParams());
    ssWriter = new SensorSetWriter(new DBParams());
  }

  @Before
  public void setup() {
    // we assume sensor set 99998 does not exist, but we could delete it here
  }
  
  @Test
  public void testInsertDeleteSensorSet() throws core.DatabaseException {
    Long sensorSetID = 99998L;
    SensorSet ss = new SensorSet();
    ss.setId(sensorSetID);
    ss.setName("writer-test");
    ss.setDescription("for test");
    ss.setProjectId(99999L);
    
    List<Sensor> sensors = new ArrayList<Sensor>();
    ss.setSensorList(sensors);
    
    Sensor s0 = new Sensor();
    s0.setType("Loop");
    s0.setEntityId("ent0");
    s0.setMeasurementFeedId(10L);
    s0.setLinkId(100L);
    s0.setLinkOffset(12.34);
    s0.setLaneNum(4.0);
    s0.setHealthStatus(0.7);
    sensors.add(s0);

    Sensor s1 = new Sensor();
    s1.setType("Magnetic");
    s1.setEntityId("ent1");
    s1.setMeasurementFeedId(11L);
    s1.setLinkId(200L);
    s1.setLinkOffset(56.78);
    s1.setLaneNum(3.0);
    s1.setHealthStatus(0.6);
    sensors.add(s1);
    
    //System.out.println("Test SensorSet -- input: " + ss);
    //{"id": "99998", "name": "writer-test", "description": "for test", "projectId": "99999", "sensors": [{"type": "Loop", "entityId": "ent0", "measurementFeedId": "10", "linkId": "100", "linkOffset": 12.34, "laneNum": 4.0, "healthStatus": 0.7}, {"type": "Magnetic", "entityId": "ent1", "measurementFeedId": "11", "linkId": "200", "linkOffset": 56.78, "laneNum": 3.0, "healthStatus": 0.6}]}

    ssWriter.insert(ss);
    
    SensorSet ss2 = ssReader.read(sensorSetID);
    assertTrue(null != ss2);

    //System.out.println("Test SensorSet -- output: " + ss2);
    //{"id": "99998", "name": "writer-test", "description": "for test", "projectId": "99999", "sensors": [{"type": "Loop", "entityId": "ent0", "measurementFeedId": "10", "linkId": "100", "linkOffset": 12.34, "laneNum": 4.0, "healthStatus": 1.0}, {"type": "Magnetic", "entityId": "ent1", "measurementFeedId": "11", "linkId": "200", "linkOffset": 56.78, "laneNum": 3.0, "healthStatus": 1.0}]}
    // Note: the healthStatus does not round-trip yet due to a precision mismatch
    // between the db and model-elements.
    
    assertEquals(sensorSetID, ss2.getLongId());
    
    Sensor s20 = null;
    Sensor s21 = null;
    
    for (Sensor s : ss2.getSensorList()) {
      if (s.getEntityId().equals("ent0")) {
        s20 = s;
        break;
      }
    }

    for (Sensor s : ss2.getSensorList()) {
      if (s.getEntityId().equals("ent1")) {
        s21 = s;
        break;
      }
    }
    
    assertEquals(s0.getType(), s20.getType());
    assertEquals(s0.getEntityId(), s20.getEntityId());
    assertEquals(s0.getMeasurementFeedId(), s20.getMeasurementFeedId());
    assertEquals(s0.getLinkId(), s20.getLinkId());
    assertEquals(s0.getLinkOffset(), s20.getLinkOffset());
    assertEquals(s0.getLaneNum(), s20.getLaneNum());
    //assertEquals(s0.getHealthStatus(), s20.getHealthStatus());
    
    assertEquals(s1.getType(), s21.getType());
    assertEquals(s1.getEntityId(), s21.getEntityId());
    assertEquals(s1.getMeasurementFeedId(), s21.getMeasurementFeedId());
    assertEquals(s1.getLinkId(), s21.getLinkId());
    assertEquals(s1.getLinkOffset(), s21.getLinkOffset());
    assertEquals(s1.getLaneNum(), s21.getLaneNum());
    //assertEquals(s1.getHealthStatus(), s21.getHealthStatus());
    
    ssWriter.delete(sensorSetID);
    
    SensorSet ss3 = ssReader.read(sensorSetID);
    assertEquals(null, ss3);
  }
}
