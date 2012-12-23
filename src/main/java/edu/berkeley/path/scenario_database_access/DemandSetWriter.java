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
 * Implements methods for writing DemandSets to a database.
 * @see DBParams
 * @author vjoel
 */
public class DemandSetWriter extends WriterBase {
  public DemandSetWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public DemandSetWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given demand set into the database.
   * 
   * @param demandSet  the demand set
   */
  public void insert(DemandSet demandSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("DemandSet insert transaction beginning on demandSet.id=" + demandSet.getId());
      
      insertWithDependents(demandSet);

      Monitor.debug("DemandSet insert transaction committing on demandSet.id=" + demandSet.getId());
      dbw.transactionCommit();
      Monitor.debug("DemandSet insert transaction committed on demandSet.id=" + demandSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("DemandSet insert transaction rollback on demandSet.id=" + demandSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert demandSet.id=" + demandSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Insert the given demandSet into the database, including dependent objects.
   * 
   * @param demandSet  the demandSet
   */
  public void insertWithDependents(DemandSet demandSet) throws DatabaseException {
    insertRow(demandSet);
    insertDependents(demandSet);
  }
  
  private void insertDependents(DemandSet demandSet) throws DatabaseException {
    DemandProfileWriter dpWriter = new DemandProfileWriter(dbParams, dbw);
    long demandSetID = demandSet.getLongId();
    
    dpWriter.insertProfiles(demandSet.getProfileMap(), demandSetID);
  }

  /**
   * Insert just the demandSet row into the database. Ignores dependent objects.
   * 
   * If the set's id is null, choose a new sequential id, insert with that id,
   * and assign that id to the set's id.
   * 
   * @param demandSet  the demandSet
   */
  public void insertRow(DemandSet demandSet) throws DatabaseException {
    String query = "insert_demandSet_" + demandSet.getId();
    dbw.psCreate(query,
      "INSERT INTO VIA.DEMAND_SETS (ID, NAME, DESCRIPTION, PROJECT_ID) VALUES(?, ?, ?, ?)"
    );
  
    try {
      dbw.psClearParams(query);

      if (demandSet.getId() == null) {
        DemandSetReader dsr = new DemandSetReader(dbParams);
        demandSet.setId(dsr.getNextID());
      }
    
      dbw.psSetBigInt(query, 1, demandSet.getLongId());
      
      dbw.psSetVarChar(query, 2,
        demandSet.getName() == null ? null : demandSet.getName().toString());
      
      dbw.psSetVarChar(query, 3,
        demandSet.getDescription() == null ? null : demandSet.getDescription().toString());
      
      dbw.psSetBigInt(query, 4,
        demandSet.getProjectId() == null ? null : demandSet.getLongProjectId());

      dbw.psUpdate(query);
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given demandSet in the database.
   * 
   * @param demandSet  the demandSet
   */
  public void update(DemandSet demandSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("DemandSet update transaction beginning on demandSet.id=" + demandSet.getId());
      
      updateWithDependents(demandSet);

      dbw.transactionCommit();
      Monitor.debug("DemandSet update transaction committing on demandSet.id=" + demandSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("DemandSet update transaction rollback on demandSet.id=" + demandSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update demandSet.id=" + demandSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Update the given demandSet in the database, including dependent objects, such as
   * demand profiles and demands.
   * Note the pre-existing dependents in the database are deleted first.
   * 
   * @see #write() if you want a transaction and logging around the operation.
   * 
   * @param demandSet  the demandSet
   */
  public void updateWithDependents(DemandSet demandSet) throws DatabaseException {
    long demandSetID = demandSet.getLongId();

    deleteDependents(demandSetID);
    updateRow(demandSet);
    insertDependents(demandSet);
  }

  /**
   * Update just the demandSet row into the database. Ignores dependent objects.
   * 
   * @param demandSet  the demandSet
   */
  public void updateRow(DemandSet demandSet) throws DatabaseException {
    String query = "update_demandSet_" + demandSet.getId();
    dbw.psCreate(query,
      "UPDATE VIA.DEMAND_SETS SET NAME = ?, DESCRIPTION = ? WHERE ID = ?"
    );
    // Note: do not update the project id. Must use separate API to move
    // this to a different project.
    
    try {
      dbw.psClearParams(query);

      dbw.psSetVarChar(query, 1,
        demandSet.getName() == null ? null : demandSet.getName().toString());
      
      dbw.psSetVarChar(query, 2,
        demandSet.getDescription() == null ? null : demandSet.getDescription().toString());

      dbw.psSetBigInt(query, 3, demandSet.getLongId());
      
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "DemandSet not unique: there exist " + rows + " with id=" + demandSet.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete the given demandSet ID from the database, and all dependent rows.
   * 
   * @param demandSetID  the demandSet ID
   */
  public void delete(long demandSetID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("DemandSet delete transaction beginning on demandSet.id=" + demandSetID);
      
      deleteDependents(demandSetID);
      deleteRow(demandSetID);

      dbw.transactionCommit();
      Monitor.debug("DemandSet delete transaction committing on demandSet.id=" + demandSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("DemandSet delete transaction rollback on demandSet.id=" + demandSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete demandSet.id=" + demandSetID, timeCommit - timeBegin);
  }

  /**
   * Delete just the demandSet row from the database. Ignores dependent objects.
   * 
   * @param demandSet  the demandSet
   */
  public void deleteRow(long demandSetID) throws DatabaseException {
    String query = "delete_demandSet_" + demandSetID;
    dbw.psCreate(query,
      "DELETE FROM VIA.DEMAND_SETS WHERE ID = ?"
    );
    
    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, demandSetID);
      
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "DemandSet not unique: there exist " + rows + " with id=" + demandSetID, dbw, query);
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
   * @param demandSetID  the demandSet ID
   */
  private void deleteDependents(long demandSetID) throws DatabaseException {
    DemandProfileWriter dpWriter = new DemandProfileWriter(dbParams, dbw);
    
    dpWriter.deleteAllProfiles(demandSetID);
  }
}
