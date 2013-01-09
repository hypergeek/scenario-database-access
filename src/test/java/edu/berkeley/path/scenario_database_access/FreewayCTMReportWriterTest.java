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
 * Tests methods for writng reports to a database (and reading them as well).
 * 
 * These tests use synthetic data in the debug table.
 * 
 * @author vjoel
 */
public class FreewayCTMReportWriterTest {
  static FreewayCTMReportReader reportReader;
  static FreewayCTMReportWriter reportWriter;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    reportReader = new FreewayCTMReportReader(new DBParams());
    reportWriter = new FreewayCTMReportWriter(new DBParams());
  }

  @Before
  public void setup() {
  }
  
  /**
   * Using a minimal report, with no link-mapped data, make sure we can
   * write, read, and delete it.
   **/
  @Test
  public void testWriteReadDeleteMinimal() throws core.DatabaseException {
    FreewayCTMReport report = new FreewayCTMReport();
    
    Long runId = 99999L;
    Long networkId = 99999L;
    org.joda.time.DateTime time = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         1970,  1,  2,  3, 45,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    report.setRunId(runId);
    report.setNetworkId(networkId);
    report.setTime(DateTime.fromJoda(time));
    
    // gotta have some data or else nothing will be written, so give it a
    // dummy FD map.
    
    report.setFd(new FDMap());
    Map<String,FD> fdMap = ((FDMap)report.getFd()).getFdMap();
    FD fd = new FD();
    
    Long linkId = 99999L;
    fdMap.put(linkId.toString(), fd);
    
    fd.setFreeFlowSpeed(12.34);
    
    reportWriter.insertDebug(report);
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         1970,  1,  2,  3,  0,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardMinutes(60);
    
    Interval interval = new Interval(timeBegin, dt);
    
    List<FreewayCTMReport> reports;
    reports = reportReader.read(networkId, runId, interval, true);
    //System.out.println(reports);
    //[{"runId": 99999, "networkId": "99999", "time": {"milliseconds": 128700000}, "mean": {"linkState": null, "queueLength": null, "linkFlowState": null}, "stdDev": {"linkState": null, "queueLength": null, "linkFlowState": null}, "fd": {"fd": {"99999": {"freeFlowSpeed": 12.34, "criticalSpeed": null, "congestionWaveSpeed": null, "capacity": null, "jamDensity": null, "capacityDrop": null, "freeFlowSpeedStd": null, "congestionWaveSpeedStd": null, "capacityStd": null}}}, "includesFlows": false}]
    
    assertEquals(1, reports.size());
    
    FreewayCTMReport r0 = reports.get(0);
    assertEquals(report.getRunId(), r0.getRunId());
    assertEquals(report.getNetworkId(), r0.getNetworkId());
    assertEquals(report.getTime().getMilliseconds(), r0.getTime().getMilliseconds());

    Map<String,FD> fdMap0 = ((FDMap)r0.getFd()).getFdMap();
    assertEquals(
      fdMap.get(linkId.toString()).getFreeFlowSpeed(),
      fdMap0.get(linkId.toString()).getFreeFlowSpeed());

    Integer rows = reportWriter.delete(networkId, runId, interval, true);
    assertEquals((Integer)1, rows);

    reports = reportReader.read(networkId, runId, interval, true);
    assertEquals(0, reports.size());
  }
  
  /**
   * Test that multiple reports are retrieved in time order, and deleted
   * correctly.
   **/
  @Test
  public void testMultipleReportOrder() throws core.DatabaseException {
    FreewayCTMReport report = new FreewayCTMReport();
    
    Long runId = 99999L;
    Long networkId = 99999L;
    
    report.setRunId(runId);
    report.setNetworkId(networkId);
    
    // gotta have some data or else nothing will be written, so give it a
    // dummy FD map.
    
    report.setFd(new FDMap());
    Map<String,FD> fdMap = ((FDMap)report.getFd()).getFdMap();
    FD fd = new FD();
    
    Long linkId = 99999L;
    fdMap.put(linkId.toString(), fd);
    
    for (int i = 0; i < 10; i++) {
      fd.setFreeFlowSpeed(100.0 + i);

      org.joda.time.DateTime time = new org.joda.time.DateTime(
        // YYYY, MM, DD, HH, MM
           1970,  1,  2,  3, 45 + i,
        org.joda.time.DateTimeZone.forID("America/Los_Angeles")
      );
      report.setTime(DateTime.fromJoda(time));

      reportWriter.insertDebug(report);
    }
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         1970,  1,  2,  3,  0,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardMinutes(60);
    
    Interval interval = new Interval(timeBegin, dt);
    
    List<FreewayCTMReport> reports;
    reports = reportReader.read(networkId, runId, interval, true);
    //System.out.println(reports);
    
    assertEquals(10, reports.size());
    
    FreewayCTMReport r5 = reports.get(5);
    assertEquals((Long)(128700000L + 5 * 60 * 1000), r5.getTime().getMilliseconds());

    // check that the list is sorted
    FreewayCTMReport prev = null;
    for (FreewayCTMReport rep : reports) {
      if (prev == null) {
        prev = rep;
      }
      else {
        assertTrue(
          prev.getJodaTime().isBefore(
            rep.getJodaTime()
        ));
      }
    }

    Integer rows = reportWriter.delete(networkId, runId, interval, true);
    assertEquals((Integer)10, rows);

    reports = reportReader.read(networkId, runId, interval, true);
    assertEquals(0, reports.size());
  }
  
  /**
   * Test that a single report with multiple rows, involving link flow, speed,
   * and density, as well as origin queue lengths and FDs, are
   * written and read correctly.
   **/
  @Test
  public void testWriteReadDeleteComplex() throws core.DatabaseException {
    FreewayCTMReport report = new FreewayCTMReport();
    
    Long runId = 99999L;
    Long networkId = 99999L;
    org.joda.time.DateTime time = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         1970,  1,  2,  4, 30,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    report.setRunId(runId);
    report.setNetworkId(networkId);
    report.setTime(DateTime.fromJoda(time));
    
    // assume a two-link network (the links don't have to exist)
    Long link1Id = 101L;
    Long link2Id = 102L;
    
    // set some FD values
    report.setFd(new FDMap());
    Map<String,FD> fdMap = ((FDMap)report.getFd()).getFdMap();
/*
    FD fd1 = new FD();
    fdMap.put(link1Id.toString(), fd1);
    fd1.setCapacity(12.34);
    
    FD fd2 = new FD();
    fdMap.put(link2Id.toString(), fd2);
    fd2.setJamDensity(45.67);
*/
    FreewayCTMState mean = new FreewayCTMState();
    report.setMean(mean);

    FreewayCTMState stdDev = new FreewayCTMState();
    report.setStdDev(stdDev);

    // set some queue values (mean and stdDev), assuming link 101 is an origin
    Map<CharSequence,Double> meanQueueLengthMap = new HashMap<CharSequence,Double>();
    mean.setQueueLength(meanQueueLengthMap);
    meanQueueLengthMap.put(link1Id.toString(), 1.23);
    
    Map<CharSequence,Double> stdDevQueueLengthMap = new HashMap<CharSequence,Double>();
    stdDev.setQueueLength(stdDevQueueLengthMap);
    stdDevQueueLengthMap.put(link1Id.toString(), 4.56);
    
    // set some link state values, assuming link 102 is not an origin
    
    // mean link state
    Map<CharSequence,FreewayLinkState> meanLinkStateMap =
      new HashMap<CharSequence,FreewayLinkState>();
    mean.setLinkStateMap(meanLinkStateMap);
      
    // std dev link state
    Map<CharSequence,FreewayLinkState> stdDevLinkStateMap =
      new HashMap<CharSequence,FreewayLinkState>();
    stdDev.setLinkStateMap(stdDevLinkStateMap);
    
    // mean flow
    Map<CharSequence,FreewayLinkFlowState> meanLinkFlowStateMap =
      new HashMap<CharSequence,FreewayLinkFlowState>();
    mean.setLinkFlowStateMap(meanLinkFlowStateMap);

    // std dev flow
    Map<CharSequence,FreewayLinkFlowState> stdDevLinkFlowStateMap =
      new HashMap<CharSequence,FreewayLinkFlowState>();
    stdDev.setLinkFlowStateMap(stdDevLinkFlowStateMap);

    // populate the maps for link 102

    FreewayLinkState meanLinkState = new FreewayLinkState();
    meanLinkState.setDensity(100.1);
    meanLinkState.setVelocity(10.1);
    meanLinkStateMap.put(link2Id.toString(), meanLinkState);
    
    FreewayLinkState stdDevLinkState = new FreewayLinkState();
    stdDevLinkState.setDensity(200.1);
    stdDevLinkState.setVelocity(20.1);
    stdDevLinkStateMap.put(link2Id.toString(), stdDevLinkState);
    
    FreewayLinkFlowState meanFlowState = new FreewayLinkFlowState();
    meanFlowState.setInFlow(11.2);
    meanFlowState.setOutFlow(12.3);
    meanLinkFlowStateMap.put(link2Id.toString(), meanFlowState);

    FreewayLinkFlowState stdDevFlowState = new FreewayLinkFlowState();
    stdDevFlowState.setInFlow(21.2);
    stdDevFlowState.setOutFlow(22.3);
    stdDevLinkFlowStateMap.put(link2Id.toString(), stdDevFlowState);

    // write the rows
    reportWriter.insertDebug(report);
    
    org.joda.time.DateTime timeBegin = new org.joda.time.DateTime(
      // YYYY, MM, DD, HH, MM
         1970,  1,  2,  4,  30,
      org.joda.time.DateTimeZone.forID("America/Los_Angeles")
    );
    
    org.joda.time.Duration dt = org.joda.time.Duration.standardMinutes(1);
    
    Interval interval = new Interval(timeBegin, dt);
    
    List<FreewayCTMReport> reports;
    reports = reportReader.read(networkId, runId, interval, true);
    System.out.println(reports);
    
    assertEquals(1, reports.size());
    
    FreewayCTMReport r0 = reports.get(0);
    assertEquals(report.getRunId(), r0.getRunId());
    assertEquals(report.getNetworkId(), r0.getNetworkId());
    assertEquals(report.getTime().getMilliseconds(), r0.getTime().getMilliseconds());

/*
    Map<String,FD> fdMap0 = ((FDMap)r0.getFd()).getFdMap();
    assertEquals(
      fdMap.get(linkId.toString()).getFreeFlowSpeed(),
      fdMap0.get(linkId.toString()).getFreeFlowSpeed());

    Integer rows = reportWriter.delete(networkId, runId, interval, true);
    assertEquals((Integer)1, rows);

    reports = reportReader.read(networkId, runId, interval, true);
    assertEquals(0, reports.size());
*/

    Integer rows = reportWriter.delete(networkId, runId, interval, true);
//    assertEquals((Integer)10, rows);

    reports = reportReader.read(networkId, runId, interval, true);
    assertEquals(0, reports.size());
  }
}
