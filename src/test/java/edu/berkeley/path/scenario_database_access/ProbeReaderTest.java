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
 * Tests methods for reading probes from a database.
 * @author vjoel
 */
public class ProbeReaderTest {
  static ProbeReader probeReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    probeReader = new ProbeReader(new DBParams());
  }

  @Before
  public void setup() {
  }
  
  @Test
  public void testRead() throws core.DatabaseException {
    List<PifProbeCoord> probes;
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         2012, 12, 14, 18, 29,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardSeconds(30);
    
    Interval interval = new Interval(timeBegin, dt);
    
    // some data known to exist at one time, YMMV
    Long runId = 18040L;
    Long networkId = 100000L;
    Long linkId = 707046366L;
    
    probes = probeReader.read(runId, networkId, linkId, interval);
    //System.out.println(probes);
    
    // this was correct, but of course might not always be...
    assertEquals(1, probes.size());
    
    // check that the list is sorted
    PifProbeCoord prev = null;
    for (PifProbeCoord probe : probes) {
      if (prev == null) {
        prev = probe;
      }
      else {
        assertTrue(
          prev.getJodaTime().isBefore(
            probe.getJodaTime()
        ));
      }
    }
    
    // this was correct, but of course might not always be...
    assertEquals((Double)111.71988, probes.get(0).getOffset());
    assertEquals((Double)98.0, probes.get(0).getSpeed());
    assertEquals((Double)1.0, probes.get(0).getProbability());
  }
}
