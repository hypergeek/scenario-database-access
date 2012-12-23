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

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;
import java.util.HashSet;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for writing SensorSets to a database.
 * @see DBParams
 * @author vjoel
 */
public class SensorSetWriter extends WriterBase {
  public SensorSetWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public SensorSetWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given sensor set into the database.
   * 
   * @param sensorSet  the sensor set
   */
  public void insert(SensorSet sensorSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("SensorSet insert transaction beginning on sensorSet.id=" + sensorSet.getId());
      
      insertWithDependents(sensorSet);

      Monitor.debug("SensorSet insert transaction committing on sensorSet.id=" + sensorSet.getId());
      dbw.transactionCommit();
      Monitor.debug("SensorSet insert transaction committed on sensorSet.id=" + sensorSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("SensorSet insert transaction rollback on sensorSet.id=" + sensorSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert sensorSet.id=" + sensorSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Insert the given sensorSet into the database, including dependent objects.
   * 
   * @param sensorSet  the sensorSet
   */
  public void insertWithDependents(SensorSet sensorSet) throws DatabaseException {
    insertRow(sensorSet);
    insertDependents(sensorSet);
  }
  
  private void insertDependents(SensorSet sensorSet) throws DatabaseException {
    long sensorSetID = sensorSet.getLongId();
    
    insertSensors(sensorSet.getSensorList(), sensorSetID);
  }

  /**
   * Insert just the sensorSet row into the database. Ignores dependent objects.
   * 
   * If the set's id is null, choose a new sequential id, insert with that id,
   * and assign that id to the set's id.
   * 
   * @param sensorSet  the sensorSet
   */
  public void insertRow(SensorSet sensorSet) throws DatabaseException {
    String query = "insert_sensorSet_" + sensorSet.getId();
    dbw.psCreate(query,
      "INSERT INTO VIA.SENSOR_SETS (ID, NAME, DESCRIPTION, PROJECT_ID) VALUES(?, ?, ?, ?)"
    );
  
    try {
      dbw.psClearParams(query);

      if (sensorSet.getId() == null) {
        SensorSetReader ssr = new SensorSetReader(dbParams);
        sensorSet.setId(ssr.getNextID());
      }
    
      dbw.psSetBigInt(query, 1, sensorSet.getLongId());
      
      dbw.psSetVarChar(query, 2,
        sensorSet.getName() == null ? null : sensorSet.getName().toString());
      
      dbw.psSetVarChar(query, 3,
        sensorSet.getDescription() == null ? null : sensorSet.getDescription().toString());
      
      dbw.psSetBigInt(query, 4,
        sensorSet.getProjectId() == null ? null : sensorSet.getLongProjectId());

      dbw.psUpdate(query);
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Insert a list of sensors belonging to a set.
   * This is intended to be called from @see insert(), so it does
   * not set up a transaction of its own.
   * 
   * @param sensors list of sensors.
   * @param sensorSetID ID of the set.
   */
  public void insertSensors(List<Sensor> sensors, long sensorSetID) throws DatabaseException {
    String query = "insert_sensors_in_sensorSet_" + sensorSetID;
    
    dbw.psCreate(query,
      "INSERT INTO VIA.SENSORS " +
        "(ID, SENSOR_TYPE_ID, SENSOR_SET_ID, " +
         "ENTITY_ID, DATA_FEED_ID, LINK_ID, " +
         "LINK_OFFSET, LANE_NUM, HEALTH_STATUS) " +
        "SELECT " +
            "VIA.SEQ_SENSORS_ID.nextVal, " +
            "ID, " +
            "?, ?, ?, ?, ?, ?, ? " +
          "FROM VIA.SENSOR_TYPES " +
          "WHERE NAME = ? "
        // geom
    );

    try {
      dbw.psClearParams(query);
      
      for (Sensor sensor : sensors) {
        int i = 0;

        dbw.psSetBigInt(query, ++i, sensorSetID);
        dbw.psSetVarChar(query, ++i, sensor.getEntityId().toString());
        dbw.psSetBigInt(query, ++i, sensor.getLongMeasurementFeedId());
        // geom
        dbw.psSetBigInt(query, ++i, sensor.getLongLinkId());
        dbw.psSetDouble(query, ++i, sensor.getLinkOffset());
        
        // these two are not consistent between ME and DB
        dbw.psSetDouble(query, ++i, sensor.getLaneNum());
        dbw.psSetDouble(query, ++i, sensor.getHealthStatus());
        
        dbw.psSetVarChar(query, ++i, sensor.getType().toString());
        
        dbw.psUpdate(query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given sensorSet in the database.
   * 
   * @param sensorSet  the sensorSet
   */
  public void update(SensorSet sensorSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("SensorSet update transaction beginning on sensorSet.id=" + sensorSet.getId());
      
      updateWithDependents(sensorSet);

      dbw.transactionCommit();
      Monitor.debug("SensorSet update transaction committing on sensorSet.id=" + sensorSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("SensorSet update transaction rollback on sensorSet.id=" + sensorSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update sensorSet.id=" + sensorSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Update the given sensorSet in the database, including dependent objects, such as
   * sensor profiles and sensors.
   * Note the pre-existing dependents in the database are deleted first.
   * 
   * @see #write() if you want a transaction and logging around the operation.
   * 
   * @param sensorSet  the sensorSet
   */
  public void updateWithDependents(SensorSet sensorSet) throws DatabaseException {
    long sensorSetID = sensorSet.getLongId();

    deleteDependents(sensorSetID);
    updateRow(sensorSet);
    insertDependents(sensorSet);
  }

  /**
   * Update just the sensorSet row into the database. Ignores dependent objects.
   * 
   * @param sensorSet  the sensorSet
   */
  public void updateRow(SensorSet sensorSet) throws DatabaseException {
    String query = "update_sensorSet_" + sensorSet.getId();
    dbw.psCreate(query,
      "UPDATE VIA.SENSOR_SETS SET NAME = ?, DESCRIPTION = ? WHERE ID = ?"
    );
    // Note: do not update the project id. Must use separate API to move
    // this to a different project.
    
    try {
      dbw.psClearParams(query);

      dbw.psSetVarChar(query, 1,
        sensorSet.getName() == null ? null : sensorSet.getName().toString());
      
      dbw.psSetVarChar(query, 2,
        sensorSet.getDescription() == null ? null : sensorSet.getDescription().toString());

      dbw.psSetBigInt(query, 3, sensorSet.getLongId());
      
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "SensorSet not unique: there exist " + rows + " with id=" + sensorSet.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete the given sensorSet ID from the database, and all dependent rows.
   * 
   * @param sensorSetID  the sensorSet ID
   */
  public void delete(long sensorSetID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("SensorSet delete transaction beginning on sensorSet.id=" + sensorSetID);
      
      deleteDependents(sensorSetID);
      deleteRow(sensorSetID);

      dbw.transactionCommit();
      Monitor.debug("SensorSet delete transaction committing on sensorSet.id=" + sensorSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("SensorSet delete transaction rollback on sensorSet.id=" + sensorSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete sensorSet.id=" + sensorSetID, timeCommit - timeBegin);
  }

  /**
   * Delete just the sensorSet row from the database. Ignores dependent objects.
   * 
   * @param sensorSet  the sensorSet
   */
  public void deleteRow(long sensorSetID) throws DatabaseException {
    String query = "delete_sensorSet_" + sensorSetID;
    dbw.psCreate(query,
      "DELETE FROM VIA.SENSOR_SETS WHERE ID = ?"
    );
    
    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, sensorSetID);
      
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "SensorSet not unique: there exist " + rows + " with id=" + sensorSetID, dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete just the dependent objects.
   * 
   * @param sensorSetID  the sensorSet ID
   */
  private void deleteDependents(long sensorSetID) throws DatabaseException {
    String query = "delete_sensors_" + sensorSetID;
    dbw.psCreate(query,
      "DELETE FROM VIA.SENSORS WHERE SENSOR_SET_ID = ?"
    );
    
    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, sensorSetID);
      
      long rows = dbw.psUpdate(query);
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
}
