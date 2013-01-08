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
 * Tests methods for reading reports from a database.
 * 
 * These reader tests simply use existing data in the real (non-debug) table.
 * If that data changes, the assertions here may fail.
 * 
 * The corresponding FreewayCTMReportWriterTest uses the debug table, writes
 * its own data, and is therefore insulated from this problem. However, it only
 * tests writing to (and reading from) the debug table.
 * 
 * @author vjoel
 */
public class FreewayCTMReportReaderTest {
  static FreewayCTMReportReader reportReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    reportReader = new FreewayCTMReportReader(new DBParams());
  }

  @Before
  public void setup() {
  }
  
  // TODO: this test won't work until the LINK_DATA_TOTAL_DEBUG table
  // has a queue_length column.
  @Ignore
  @Test
  public void testRead() throws core.DatabaseException {
    List<FreewayCTMReport> reports;
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         2012, 12, 05, 11, 16,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardSeconds(120);
    
    Interval interval = new Interval(timeBegin, dt);
    
    // some data known to exist at one time, YMMV
    Long runId = 1L;
    Long networkId = 100000L;
    
    reports = reportReader.read(networkId, runId, interval, false);
    System.out.println(reports);
    
    // this was correct, but of course might not always be...
    assertEquals(1, reports.size());
    
    // check that the list is sorted
    FreewayCTMReport prev = null;
    for (FreewayCTMReport report : reports) {
      if (prev == null) {
        prev = report;
      }
      else {
        assertTrue(
          prev.getJodaTime().isBefore(
            report.getJodaTime()
        ));
      }
    }
    
    // this was correct, but of course might not always be...
    // TODO add some assertions here based on the data that happens to exist
  }
}
