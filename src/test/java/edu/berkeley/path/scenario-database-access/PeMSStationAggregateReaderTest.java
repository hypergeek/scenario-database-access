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
 * Tests methods for reading PeMSStationAggregate data from a database.
 * @author vjoel
 */
public class PeMSStationAggregateReaderTest {
  static PeMSStationAggregateReader saggReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    saggReader = new PeMSStationAggregateReader(new DBParams());
  }

  @Before
  public void setup() {
  }
  
  @Test
  public void testRead5Min() throws core.DatabaseException {
    List<PeMSStationAggregate> saggs;
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         2010,  1,  1,  9,  0,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardHours(1);
    
    Interval interval = new Interval(timeBegin, dt);
    
    ArrayList<Long> vdsIds = new ArrayList<Long>();
    vdsIds.add(314121L);
    vdsIds.add(400211L);
    vdsIds.add(400212L);
    
    saggs = saggReader.read(interval, vdsIds, PeMSAggregate.AggregationLevel.PEMS_5MIN);
    
    // this was correct, but of course might not always be...
    assertEquals(39, saggs.size());
    
    // check that the list is sorted
    PeMSStationAggregate prev = null;
    for (PeMSStationAggregate sagg : saggs) {
      if (prev == null) {
        prev = sagg;
      }
      else {
        assertTrue(
          prev.getJodaTimeMeasured().isBefore(
            sagg.getJodaTimeMeasured())
          ||
          prev.getJodaTimeMeasured().isEqual(
            sagg.getJodaTimeMeasured())
        );
      }
    }
  }

  @Test
  public void testRead1Hour() throws core.DatabaseException {
    List<PeMSStationAggregate> saggs;
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         2010,  1,  1,  9,  0,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardHours(1);
    
    Interval interval = new Interval(timeBegin, dt);
    
    ArrayList<Long> vdsIds = new ArrayList<Long>();
    vdsIds.add(314121L);
    vdsIds.add(400211L);
    vdsIds.add(400212L);
    
    saggs = saggReader.read(interval, vdsIds, PeMSAggregate.AggregationLevel.PEMS_1HOUR);
    
    // this was correct, but of course might not always be...
    assertEquals(6, saggs.size());
    
    // check that the list is sorted
    PeMSStationAggregate prev = null;
    for (PeMSStationAggregate sagg : saggs) {
      if (prev == null) {
        prev = sagg;
      }
      else {
        assertTrue(
          prev.getJodaTimeMeasured().isBefore(
            sagg.getJodaTimeMeasured())
          ||
          prev.getJodaTimeMeasured().isEqual(
            sagg.getJodaTimeMeasured())
        );
      }
    }
  }

  @Test
  public void testRead1Day() throws core.DatabaseException {
    List<PeMSStationAggregate> saggs;
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         2010,  1,  1,  9,  0,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardDays(1);
    
    Interval interval = new Interval(timeBegin, dt);
    
    ArrayList<Long> vdsIds = new ArrayList<Long>();
    vdsIds.add(314121L);
    vdsIds.add(400211L);
    vdsIds.add(400212L);
    
    saggs = saggReader.read(interval, vdsIds, PeMSAggregate.AggregationLevel.PEMS_1DAY);
    
    // this was correct, but of course might not always be...
    assertEquals(3, saggs.size());
    
    // check that the list is sorted
    PeMSStationAggregate prev = null;
    for (PeMSStationAggregate sagg : saggs) {
      if (prev == null) {
        prev = sagg;
      }
      else {
        assertTrue(
          prev.getJodaTimeMeasured().isBefore(
            sagg.getJodaTimeMeasured())
          ||
          prev.getJodaTimeMeasured().isEqual(
            sagg.getJodaTimeMeasured())
        );
      }
    }
  }
}
