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
import java.util.HashMap;
import java.util.Arrays;
import java.util.HashSet;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Implements methods for writing Scenarios to a database.
 * @see DBParams
 * @author vjoel
 */
public class ScenarioWriter extends WriterBase {
  public ScenarioWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public ScenarioWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given scenario into the database.
   * Does not insert networks, profiles, or other independently existing
   * data structures associated with the scenario.
   * 
   * @param scenario  the scenario
   */
  public void insert(Scenario scenario) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Scenario insert transaction beginning on scenario.id=" + scenario.getId());
      
      insertWithDependents(scenario);

      dbw.transactionCommit();
      Monitor.debug("Scenario insert transaction committing on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Scenario insert transaction rollback on scenario.id=" + scenario.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert scenario.id=" + scenario.getId(), timeCommit - timeBegin);
  }

  /**
   * Insert the given scenario into the database.
   * Does not insert networks, profiles, or other independently existing
   * data structures associated with the scenario.
   * 
   * @param scenario  the scenario
   */
  public void insertWithDependents(Scenario scenario) throws DatabaseException {
    insertRow(scenario);
  }

  /**
   * Insert just the scenario row into the database.
   * Does not insert networks, profiles, or other independently existing
   * data structures associated with the scenario.
   * 
   * If the scenarios's id is null, choose a new sequential id, insert with that id,
   * and assign that id to the scenario's id.
   * 
   * @param scenario  the scenario
   */
  public void insertRow(Scenario scenario) throws DatabaseException {
    String query = "insert_scenario_" + scenario.getId();
    dbw.psCreate(query,
      "INSERT INTO VIA.SCENARIOS (ID, NAME, DESCRIPTION, PROJECT_ID) VALUES(?, ?, ?, ?)"
    );
  
    try {
      dbw.psClearParams(query);

      if (scenario.getId() == null) {
        ScenarioReader sr = new ScenarioReader(dbParams);
        scenario.setId(sr.getNextID());
      }
    
      dbw.psSetBigInt(query, 1, scenario.getLongId());
      
      dbw.psSetVarChar(query, 2,
        scenario.getName() == null ? null : scenario.getName().toString());
      
      dbw.psSetVarChar(query, 3,
        scenario.getDescription() == null ? null : scenario.getDescription().toString());
      
      dbw.psSetBigInt(query, 4,
        scenario.getProjectId() == null ? 1L : scenario.getLongProjectId());
        // NOTE using id=1L because the scenarios table doesn't allow nulls--should be fixed

      long rows = dbw.psUpdate(query);
      if (rows != 1) {
        throw new DatabaseException(null, "Scenario not unique: there exist " + rows + " with id=" + scenario.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given scenario in the database.
   * Does not update networks, profiles, or other independently existing
   * data structures associated with the scenario.
   * 
   * @param scenario  the scenario
   */
  public void update(Scenario scenario) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Scenario update transaction beginning on scenario.id=" + scenario.getId());
      
      updateWithDependents(scenario);

      dbw.transactionCommit();
      Monitor.debug("Scenario update transaction committing on scenario.id=" + scenario.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Scenario update transaction rollback on scenario.id=" + scenario.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update scenario.id=" + scenario.getId(), timeCommit - timeBegin);
  }

  /**
   * Update the given scenario in the database.
   * 
   * @param scenario  the scenario
   */
  public void updateWithDependents(Scenario scenario) throws DatabaseException {
    updateRow(scenario);
  }

  /**
   * Update just the scenario row into the database.
   * 
   * @param scenario  the scenario
   */
  public void updateRow(Scenario scenario) throws DatabaseException {
    String query = "update_scenario_" + scenario.getId();
    dbw.psCreate(query,
      "UPDATE VIA.SCENARIOS SET NAME = ?, DESCRIPTION = ?, PROJECT_ID = ? WHERE ID = ?"
    );
    // Note: do not update the project id. Must use separate API to move
    // this to a different project.
    
    try {
      dbw.psClearParams(query);

      dbw.psSetVarChar(query, 1,
        scenario.getName() == null ? null : scenario.getName().toString());
      
      dbw.psSetVarChar(query, 2,
        scenario.getDescription() == null ? null : scenario.getDescription().toString());

      dbw.psSetInteger(query, 3, 1); // project id
      
      dbw.psSetBigInt(query, 4, scenario.getLongId());
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Scenario not unique: there exist " + rows + " with id=" + scenario.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete the given scenario ID from the database.
   * Does not delete networks, profiles, or other independently existing
   * data structures associated with the scenario.
   * 
   * @param scenarioID  the scenario ID
   */
  public void delete(long scenarioID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("Scenario delete transaction beginning on scenario.id=" + scenarioID);
      
      deleteRow(scenarioID);
      
      dbw.transactionCommit();
      Monitor.debug("Scenario delete transaction committing on scenario.id=" + scenarioID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("Scenario delete transaction rollback on scenario.id=" + scenarioID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete scenario.id=" + scenarioID, timeCommit - timeBegin);
  }

  /**
   * Delete just the scenario row from the database.
   * 
   * @param scenario  the scenario
   */
  public void deleteRow(long scenarioID) throws DatabaseException {
    String query = "delete_scenario_" + scenarioID;
    dbw.psCreate(query,
      "DELETE FROM VIA.SCENARIOS WHERE ID = ?"
    );
    
    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, scenarioID);
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "Scenario not unique: there exist " + rows + " with id=" + scenarioID, dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
}
