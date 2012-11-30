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

import java.util.*;

import edu.berkeley.path.model_elements.*;

/**
 * Tests methods for reading PeMS stations from a database.
 * @author vjoel
 */
public class PeMSStationReaderTest {
  static PeMSStationReader stationReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    stationReader = new PeMSStationReader(new DBParams());
  }

  @Before
  public void setup() {
  }
  
  @Test
  public void testReadOneStation() throws core.DatabaseException {
    Long vdsId = 400211L;
    PeMSStation station = stationReader.read(vdsId);
    //System.out.println(station);
    //{"Id": 400211, "fwyNum": 680, "direction": "N", "district": 4, "county": "Contra Costa", "city": "Walnut Creek", "statePostmile": 13.36, "absPostmile": 44.861, "latitude": 37.889123, "longitude": -122.057695, "detectorLength": 0.35, "detectorType": "ML   ", "detectorName": "1700' N of S Main St", "laneCount": 5, "userId": [null, "DT262", "L5-N-28-680-01336", null, null]}
    
    assertEquals(vdsId, station.getId());
    assertEquals((Integer)680, station.getFwyNum());
    assertEquals("N", station.getDirection());
    assertEquals((Integer)4, station.getDistrict());
    assertEquals("Contra Costa", station.getCounty());
    assertEquals("Walnut Creek", station.getCity());
    assertEquals((Double)13.36, station.getStatePostmile());
    assertEquals((Double)44.861, station.getAbsPostmile());
    assertEquals((Double)37.889123, station.getLatitude());
    assertEquals((Double)(-122.057695), station.getLongitude());
    assertEquals((Double)0.35, station.getDetectorLength());
    assertEquals("ML   ", station.getDetectorType());
    assertEquals("1700' N of S Main St", station.getDetectorName());
    assertEquals((Integer)5, station.getLaneCount());
    assertEquals("DT262", station.getUserId().get(1));
    assertEquals("L5-N-28-680-01336", station.getUserId().get(2));
    assertEquals(null, station.getUserId().get(3));
    assertEquals(null, station.getUserId().get(4));
  }
}
