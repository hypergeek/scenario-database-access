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
 * Implements methods for reading SensorSets from a database.
 * @see DBParams
 * @author vjoel
 */
public class SensorSetReader extends ReaderBase {
  public SensorSetReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public SensorSetReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read one sensor set with the given ID from the database, plus
   * all dependent objects, namely the associated sensors.
   * 
   * @param sensorSetID  ID of the Set in the database
   * @return SensorSet
   */
  public SensorSet read(long sensorSetID) throws DatabaseException {
    SensorSet sensorSet;
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug(
        "SensorSet reader transaction beginning on sensorSet.id=" +
        sensorSetID);

      sensorSet = readWithDependents(sensorSetID);

      dbr.transactionCommit();
      Monitor.debug(
        "SensorSet reader transaction committing on sensorSet.id=" +
        sensorSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug(
          "SensorSet reader transaction rollback on sensorSet.id=" +
          sensorSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (sensorSet != null) {
      Monitor.duration(
        "Read sensorSet.id=" +
        sensorSet.getId(), timeCommit - timeBegin);
    }

    return sensorSet;
  }

  /**
   * Read the SensorSet row with the given ID from the database, plus
   * all dependent objects, namely the sensors.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param sensorSetID  ID of the sensorSet in the database
   * @return SensorSet.
   */
  public SensorSet readWithDependents(long sensorSetID) throws DatabaseException {
    SensorSet sensorSet = readRow(sensorSetID);

    if (sensorSet != null) {
      sensorSet.setSensorList(readSensors(sensorSetID));
    }
    
    return sensorSet;
  }

  /**
   * Read just the SensorSet row with the given ID from the database. Ignores
   * dependent objects.
   * 
   * @param sensorSetID  ID of the sensorSet in the database
   * @return SensorSet, with null for all dependent objects.
   */
  public SensorSet readRow(long sensorSetID) throws DatabaseException {
    String query = null;
    SensorSet sensorSet = null;
    
    try {
      query = runQuery(sensorSetID);
      sensorSet = sensorSetFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return sensorSet;
  }

  /**
   * Execute a query for the specified sensorSet.
   * 
   * @param sensorSetID  ID of the sensorSet in the database
   * @return String     query string, may be passed to psRSNext or sensorSetFromQueryRS
   */
  protected String runQuery(long sensorSetID) throws DatabaseException {
    String query = "read_sensorSet_" + sensorSetID;
    
    dbr.psCreate(query,
      "SELECT * FROM VIA.SENSOR_SETS WHERE (ID = ?)"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, sensorSetID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a sensorSet object from the result set
   * of a sensorSet query. Do not attempt to read dependent rows.
   * 
   * @param query string
   * @return SensorSet
   */
  protected SensorSet sensorSetFromQueryRS(String query) throws DatabaseException {
    SensorSet sensorSet = null;
    
    while (dbr.psRSNext(query)) {
      if (sensorSet != null) {
        throw new DatabaseException(null,
          "SensorSet not unique: " + query, dbr, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      sensorSet = new SensorSet();
      
      Long id = dbr.psRSGetBigInt(query, "ID");
      String name = dbr.psRSGetVarChar(query, "NAME");
      String desc = dbr.psRSGetVarChar(query, "DESCRIPTION");
      Long prjId = dbr.psRSGetBigInt(query, "PROJECT_ID");
      Long modstampMicros = dbr.psRSGetTimestampMicroseconds(query, "MODSTAMP");
      
      sensorSet.setId(id);
      sensorSet.setName(name);
      sensorSet.setDescription(desc);
      sensorSet.setProjectId(prjId == null ? null : prjId.toString());
      sensorSet.setModstamp(modstampMicros);

      //System.out.println("SensorSet: " + sensorSet);
    }

    return sensorSet;
  }
  
  /**
   * Read just the list of sensors associated with the given set ID.
   * 
   * @param sensorSetID  ID of the sensorSet in the database
   * @return sensorList.
   */
  protected List<Sensor> readSensors(long sensorSetID) throws DatabaseException {
    List<Sensor> sensors = new ArrayList<Sensor>();
    
    String query = null;
    
    try {
      query = runSensorQuery(sensorSetID);
      Sensor sensor;
      while (null != (sensor = sensorFromQueryRS(query))) {
        sensors.add(sensor);
      }
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return sensors;
  }

  /**
   * Execute a query for the specified sensors.
   * 
   * @param sensorSetID  ID of the sensorSet in the database
   * @return String     query string, may be passed to psRSNext or sensorFromQueryRS
   */
  protected String runSensorQuery(long sensorSetID) throws DatabaseException {
    String query = "read_sensors_" + sensorSetID;
    
    dbr.psCreate(query,
      "SELECT " +
        "SENSOR_TYPES.NAME AS SENSOR_TYPE_NAME, " +
        "ENTITY_ID, " +
        "DATA_FEED_ID, " +
        //geom
        "LINK_ID, " +
        "LINK_OFFSET, " +
        "LANE_NUM, " +
        "HEALTH_STATUS " +
      "FROM VIA.SENSORS " +
      "LEFT OUTER JOIN VIA.SENSOR_TYPES " +
        "ON VIA.SENSORS.SENSOR_TYPE_ID = VIA.SENSOR_TYPES.ID " +
      "WHERE SENSOR_SET_ID = ?"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, sensorSetID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a sensor object from the result set
   * of a sensor query. Consumes one item from the result set of the
   * query. Returns null if no more results.
   * 
   * @param query string
   * @return Sensor
   */
  protected Sensor sensorFromQueryRS(String query) throws DatabaseException {
    Sensor sensor = null;
    
    if (dbr.psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      sensor = new Sensor();
      
      String type = dbr.psRSGetVarChar(query, "SENSOR_TYPE_NAME");
      String entityId = dbr.psRSGetVarChar(query, "ENTITY_ID");
      Long feedId = dbr.psRSGetBigInt(query, "DATA_FEED_ID");
      // geom
      Long linkId = dbr.psRSGetBigInt(query, "LINK_ID");
      Double linkOffset = dbr.psRSGetDouble(query, "LINK_OFFSET");
      
      // these two are not consistent between ME and DB
      Double laneNum = dbr.psRSGetDouble(query, "LANE_NUM");
      Double healthStatus = dbr.psRSGetDouble(query, "HEALTH_STATUS");
      
      sensor.setType(type);
      sensor.setEntityId(entityId);
      sensor.setMeasurementFeedId(feedId);
      // geom
      sensor.setLinkId(linkId);
      sensor.setLinkOffset(linkOffset);
      sensor.setLaneNum(laneNum);
      sensor.setHealthStatus(healthStatus);

      //System.out.println("Sensor: " + sensor);
    }

    return sensor;
  }
  
  protected String seqQueryName() {
    return "nextSensorSetID";
  }
  
  protected String seqQuerySql() {
    return "SELECT VIA.SEQ_SENSOR_SETS_ID.nextVal AS ID FROM dual";
  }
}
