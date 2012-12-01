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

import org.joda.time.Interval;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for reading PeMS aggregate data from a database.
 * @see DBParams
 * @author vjoel
 */
public class PeMSStationAggregateReader extends ReaderBase {
  public PeMSStationAggregateReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public PeMSStationAggregateReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read all PeMS aggregate data in the given time range, having a VDS ID
   * in the given list, and at the given aggregation level. List is sorted by time.
   */
  public List<PeMSStationAggregate> read(
      Interval interval,
      List<Long> vdsIds,
      PeMSAggregate.AggregationLevel level) throws DatabaseException {

    List<PeMSStationAggregate> list;

    String pemsIdStr = "pems.{vds_id=[" +
       vdsIds.get(0) + "," + vdsIds.get(1) +
       ",...], interval=" + interval +
       ", level=" + level + "}";
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("PeMS aggregate reader transaction beginning on " + pemsIdStr);

      list = readList(interval, vdsIds, level);

      dbr.transactionCommit();
      Monitor.debug("PeMS aggregate reader transaction committing on " + pemsIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("PeMS aggregate reader transaction rollback on " + pemsIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (list != null) {
      Monitor.duration("Read " + pemsIdStr, timeCommit - timeBegin);
    }

    return list;
  }
  
  /**
   * Read all PeMS aggregate data in the given time range, having a VDS ID
   * in the given list, and at the given aggregation level. List is sorted by time.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   */
  public List<PeMSStationAggregate> readList(
      Interval interval,
      List<Long> vdsIds,
      PeMSAggregate.AggregationLevel level) throws DatabaseException {

    List<PeMSStationAggregate> list = new ArrayList<PeMSStationAggregate>();
    PeMSStationAggregate sagg;
    
    String query = null;
    
    try {
      query = runQueryAggregates(interval, vdsIds, level);
      while (null != (sagg = aggregateFromQueryRS(query, level))) {
        list.add(sagg);
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return list;
  }
    
  /**
   * Execute a query for the specified pems aggregate data.
   * 
   * @return String     query string, may be passed to psRSNext or pemsFromQueryRS
   */
  protected String runQueryAggregates(
      Interval interval,
      List<Long> vdsIds,
      PeMSAggregate.AggregationLevel level) throws DatabaseException {
    
    String query = "read_pems_aggregates";
    String yuckyuck = org.apache.commons.lang.StringUtils.join(vdsIds, ", ");
        
    dbr.psCreate(query,
      "SELECT * " +
      "FROM VIA." + level.table + " " +
      "WHERE " +
         "MEASURE_DT BETWEEN ? AND ? " +
         "AND " +
         "VDS_ID IN (" + yuckyuck + ") " +
      "ORDER BY MEASURE_DT"
    );
    
    dbr.psClearParams(query);
    dbr.psSetTimestampMilliseconds(query, 1, interval.getStartMillis());
    dbr.psSetTimestampMilliseconds(query, 2, interval.getEndMillis());
    
    // this doesn't seem to work, hence the yuck hack above:
    //dbr.psSetArrayLong(query, 3, vdsIds.toArray(new Long[vdsIds.size()]));

    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a pems aggregate object from the next item in the result set
   * of a pems aggregate query.
   * 
   * @param query string
   * @return PeMSStationAggregate
   */
  protected PeMSStationAggregate aggregateFromQueryRS(
      String query,
      PeMSAggregate.AggregationLevel level) throws DatabaseException {
    
    PeMSStationAggregate sagg = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      sagg = new PeMSStationAggregate();

      Long vdsId = dbr.psRSGetBigInt(query, "VDS_ID");
      edu.berkeley.path.model_elements.DateTime timeMeasured =
        new edu.berkeley.path.model_elements.DateTime(dbr.psRSGetTimestampMilliseconds(query, "MEASURE_DT"));
      
      sagg.setVdsId(vdsId);
      sagg.setTimeMeasured(timeMeasured);
      
      PeMSAggregate total = new PeMSAggregate();
      
      total.setSamples(dbr.psRSGetBigInt(query, "TOTAL_SAMPLES"));
      total.setObserved(dbr.psRSGetDouble(query, "PERCENT_OBSERVED"));
      total.setFlow(dbr.psRSGetDouble(query, "TOTAL_FLOW"));
      
      if (level == PeMSAggregate.AggregationLevel.PEMS_5MIN ||
          level == PeMSAggregate.AggregationLevel.PEMS_1HOUR) {
        total.setAvgOccupancy(dbr.psRSGetDouble(query, "AVG_OCC"));
        total.setAvgSpeed(dbr.psRSGetDouble(query, "AVG_SPEED"));
      }
      
      sagg.setTotal(total);

      if (level == PeMSAggregate.AggregationLevel.PEMS_1HOUR ||
          level == PeMSAggregate.AggregationLevel.PEMS_1DAY) {
        
        Map<CharSequence, Double> delay = new HashMap<CharSequence, Double>();
        
        for (Integer mph = 35; mph <= 60; mph += 5) {
          delay.put(mph.toString(), dbr.psRSGetDouble(query, "DELAY_VT_" + mph));
        }

        sagg.setDelay(delay);
      }

      if (level == PeMSAggregate.AggregationLevel.PEMS_5MIN ||
          level == PeMSAggregate.AggregationLevel.PEMS_1HOUR) {

        List<PeMSAggregate> byLane = null;

        byLane = new ArrayList<PeMSAggregate>();
        byLane.add(null);

        for (int lane = 1; lane <= 8; lane++) {
          PeMSAggregate agg = new PeMSAggregate();
          
          String prefix = "LANE_" + lane + "_";
          
          agg.setFlow(dbr.psRSGetDouble(query, prefix + "FLOW"));
          agg.setAvgOccupancy(dbr.psRSGetDouble(query, prefix + "AVG_OCC"));
          agg.setAvgSpeed(dbr.psRSGetDouble(query, prefix + "AVG_SPEED"));
          
          if (level == PeMSAggregate.AggregationLevel.PEMS_5MIN) {
            agg.setSamples(dbr.psRSGetBigInt(query, prefix + "SAMPLES"));
            String obs = dbr.psRSGetVarChar(query, prefix + "OBSERVED");
            if (obs.equals("1")) {
              agg.setObserved(100.0);
            }
            else if (obs.equals("0")) {
              agg.setObserved(0.0);
            }
            else {
              System.out.println("Warning: unrecognized 'observed' value: '" + obs + "'.");
              // throw? log warning?
            }
          }
          
          byLane.add(agg);
        }

        sagg.setByLaneList(byLane);
      }
    }

    return sagg;
  }
}
