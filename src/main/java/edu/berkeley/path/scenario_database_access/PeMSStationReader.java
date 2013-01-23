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

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for reading PeMS data from a database.
 * @see DBParams
 * @author vjoel
 */
public class PeMSStationReader extends ReaderBase {
  public PeMSStationReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public PeMSStationReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read the PeMS station with the given ID.
   */
  public PeMSStation read(Long vdsId) throws DatabaseException {
    PeMSStation station;
    String pemsIdStr = "pems.vds_id=" + vdsId;
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug("PeMSStation reader transaction beginning on " + pemsIdStr);

      station = readStation(vdsId);

      dbr.transactionCommit();
      Monitor.debug("PeMSStation reader transaction committing on " + pemsIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug("PeMSStation reader transaction rollback on " + pemsIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (station != null) {
      Monitor.duration("Read " + pemsIdStr, timeCommit - timeBegin);
    }

    return station;
  }
  
  /**
   * Read the PeMS station with the given ID.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   */
  public PeMSStation readStation(Long vdsId) throws DatabaseException {
    PeMSStation station = null;
    String query = null;
    
    try {
      query = runQueryStation(vdsId);
      station = stationFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return station;
  }
  
  /**
   * Execute a query for the specified pems station.
   * 
   * @return String     query string, may be passed to psRSNext or stationFromQueryRS
   */
  protected String runQueryStation(Long vdsId) throws DatabaseException {
    String query = "read_pems_station";
    
    dbr.psCreate(query,
      "SELECT " +
        "VIA.PEMS_VD_STATIONS.STATE_PM, " +
        "VIA.PEMS_VD_STATIONS.ABS_PM, " +
        "VIA.PEMS_VD_STATIONS.LATITUDE, " +
        "VIA.PEMS_VD_STATIONS.LONGITUDE, " +
        "VIA.PEMS_VD_STATIONS.DET_LENGTH, " +
        "VIA.PEMS_VD_STATIONS.DET_TYPE, " +
        "VIA.PEMS_VD_STATIONS.LANES, " +
        "VIA.PEMS_VD_STATIONS.DET_NAME, " +
        "VIA.PEMS_VD_STATIONS.USER_ID_1, " +
        "VIA.PEMS_VD_STATIONS.USER_ID_2, " +
        "VIA.PEMS_VD_STATIONS.USER_ID_3, " +
        "VIA.PEMS_VD_STATIONS.USER_ID_4, " +
        //"VIA.PEMS_VD_STATIONS.GEOM, " +
        "VIA.PEMS_VD_STATIONS.ID, " +
        "VIA.PEMS_VD_STATIONS.FWY_NUM, " +
        "VIA.PEMS_VD_STATIONS.DIRECTION, " +
        "VIA.PEMS_VD_STATIONS.DISTRICT, " +
        //"VIA.PEMS_VD_STATIONS_FW_TYPE.NAME, " + // stations table doesn't ref this
        "VIA.PEMS_VD_STATIONS_CITIES.NAME AS CITY_NAME, " +
        "VIA.PEMS_VD_STATIONS_COUNTIES.NAME AS COUNTY_NAME " +
      "FROM VIA.PEMS_VD_STATIONS " +
      "LEFT OUTER JOIN VIA.PEMS_VD_STATIONS_CITIES " +
        "ON (VIA.PEMS_VD_STATIONS.CITY = VIA.PEMS_VD_STATIONS_CITIES.ID) " +
      "LEFT OUTER JOIN VIA.PEMS_VD_STATIONS_COUNTIES " +
        "ON (VIA.PEMS_VD_STATIONS.COUNTY = VIA.PEMS_VD_STATIONS_COUNTIES.ID) " +
      "WHERE " +
        "PEMS_VD_STATIONS.ID = ? "
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, vdsId);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a pems station from the next item in the result set
   * of a query.
   * 
   * @param query string
   * @return PeMSStation
   */
  protected PeMSStation stationFromQueryRS(String query) throws DatabaseException {
    PeMSStation station = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      station = new PeMSStation();
      
      Long    id              = dbr.psRSGetBigInt(query, "ID");
      Integer fwyNum          = dbr.psRSGetInteger(query, "FWY_NUM");
      String  direction       = dbr.psRSGetVarChar(query, "DIRECTION");
      Integer district        = dbr.psRSGetInteger(query, "DISTRICT");
      String  county          = dbr.psRSGetVarChar(query, "COUNTY_NAME");
      String  city            = dbr.psRSGetVarChar(query, "CITY_NAME");
      Double  statePostmile   = dbr.psRSGetDouble(query, "STATE_PM");
      Double  absPostmile     = dbr.psRSGetDouble(query, "ABS_PM");
      Double  latitude        = dbr.psRSGetDouble(query, "LATITUDE");
      Double  longitude       = dbr.psRSGetDouble(query, "LONGITUDE");
      // note: if we start using geom column instead of lat/lng, this should
      // be rewritten as in the Node reader.
      Double  detectorLength  = dbr.psRSGetDouble(query, "DET_LENGTH");
      String  detectorType    = dbr.psRSGetVarChar(query, "DET_TYPE");
      String  detectorName    = dbr.psRSGetVarChar(query, "DET_NAME");
      Integer laneCount       = dbr.psRSGetInteger(query, "LANES");
      
      ArrayList<CharSequence> userId = new ArrayList<CharSequence>();
      userId.add(null); // dummy for USER_ID_0
      for (int i = 1; i <= 4; i++) {
        userId.add(dbr.psRSGetVarChar(query, "USER_ID_" + i));
      }

      station.setId(id);
      station.setFwyNum(fwyNum);
      station.setDirection(direction);
      station.setDistrict(district);
      station.setCounty(county);
      station.setCity(city);
      station.setStatePostmile(statePostmile);
      station.setAbsPostmile(absPostmile);
      station.setLatitude(latitude);
      station.setLongitude(longitude);
      station.setDetectorLength(detectorLength);
      station.setDetectorType(detectorType);
      station.setDetectorName(detectorName);
      station.setLaneCount(laneCount);
      station.setUserId(userId);
    }

    return station;
  }
}
