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

import org.joda.time.Interval;

/**
 * Tests methods for reading PeMS from a database.
 * @author vjoel
 */
public class PeMSReaderTest {
  static PeMSReader pemsReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    pemsReader = new PeMSReader(new DBParams());
  }

  @Before
  public void setup() {
  }
  
  @Test
  public void testReadOneStation() throws core.DatabaseException {
    PeMSProfile profile;
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         2012, 11, 29,  9,  0,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardHours(1);
    
    Interval interval = new Interval(timeBegin, dt);
    
    Long vdsId = 400211L;
    
    profile = pemsReader.read(interval, vdsId);
    //System.out.println(profile);
    
    List<PeMS> pemsList = profile.getPemsList();
    
    // this was correct, but of course might not always be...
    assertEquals(6, pemsList.size());
    
    // check that the list is sorted
    PeMS prev = null;
    for (PeMS pems : pemsList) {
      if (prev == null) {
        prev = pems;
      }
      else {
        assertTrue(
          prev.getJodaTimeMeasured().isBefore(
            pems.getJodaTimeMeasured()
        ));
      }
    }
  }

  @Test
  public void testReadStationSet() throws core.DatabaseException {
    PeMSSet set;
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         2012, 11, 29,  9,  0,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardHours(1);
    
    Interval interval = new Interval(timeBegin, dt);
    
    ArrayList<Long> vdsIds = new ArrayList<Long>();
    vdsIds.add(400211L);
    vdsIds.add(400212L);
    
    set = pemsReader.read(interval, vdsIds);
    //System.out.println(set);
    
    // this was correct, but of course might not always be...
    
    assertEquals(2, set.getPemsMapList().get(0).getMap().size());
    assertEquals(2, set.getPemsMapList().get(1).getMap().size());
    assertEquals(2, set.getPemsMapList().get(2).getMap().size());
    assertEquals(1, set.getPemsMapList().get(3).getMap().size());
    assertEquals(2, set.getPemsMapList().get(4).getMap().size());
    assertEquals(2, set.getPemsMapList().get(5).getMap().size());
    assertEquals(2, set.getPemsMapList().get(6).getMap().size());
  }
}
