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
 * Implements methods for writing FDSets to a database.
 * @see DBParams
 * @author vjoel
 */
public class FDSetWriter extends WriterBase {
  public FDSetWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public FDSetWriter(
          DBParams dbParams,
          DatabaseWriter dbWriter
          ) throws DatabaseException {
    super(dbParams, dbWriter);
  }
  
  /**
   * Insert the given fd set into the database.
   * 
   * @param fdSet  the fd set
   */
  public void insert(FDSet fdSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("FDSet insert transaction beginning on fdSet.id=" + fdSet.getId());
      
      insertWithDependents(fdSet);

      Monitor.debug("FDSet insert transaction committing on fdSet.id=" + fdSet.getId());
      dbw.transactionCommit();
      Monitor.debug("FDSet insert transaction committed on fdSet.id=" + fdSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("FDSet insert transaction rollback on fdSet.id=" + fdSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert fdSet.id=" + fdSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Insert the given fdSet into the database, including dependent objects.
   * 
   * @param fdSet  the fdSet
   */
  public void insertWithDependents(FDSet fdSet) throws DatabaseException {
    insertRow(fdSet);
    insertDependents(fdSet);
  }
  
  private void insertDependents(FDSet fdSet) throws DatabaseException {
    FDProfileWriter fdpWriter = new FDProfileWriter(dbParams, dbw);
    long fdSetID = fdSet.getLongId();
    
    fdpWriter.insertProfiles(fdSet.getProfileMap(), fdSetID);
  }

  /**
   * Insert just the fdSet row into the database. Ignores dependent objects.
   * 
   * @param fdSet  the fdSet
   */
  public void insertRow(FDSet fdSet) throws DatabaseException {
    String query = "insert_fdSet_" + fdSet.getId();
    dbw.psCreate(query,
      "INSERT INTO VIA.FUND_DIAG_SETS (ID, NAME, DESCRIPTION, FUND_DIAG_TYPE, PROJECT_ID) VALUES(?, ?, ?, ?, ?)"
    );
  
    try {
      dbw.psClearParams(query);

      dbw.psSetBigInt(query, 1, fdSet.getLongId());
      
      dbw.psSetVarChar(query, 2,
        fdSet.getName() == null ? null : fdSet.getName().toString());
      
      dbw.psSetVarChar(query, 3,
        fdSet.getDescription() == null ? null : fdSet.getDescription().toString());
      
      dbw.psSetBigInt(query, 4,
        fdSet.getType() == null ? null : ((FDType)fdSet.getType()).getLongId());
      
      dbw.psSetBigInt(query, 5,
        fdSet.getProjectId() == null ? null : fdSet.getLongProjectId());

      dbw.psUpdate(query);
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given fdSet in the database.
   * 
   * @param fdSet  the fdSet
   */
  public void update(FDSet fdSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("FDSet update transaction beginning on fdSet.id=" + fdSet.getId());
      
      updateWithDependents(fdSet);

      dbw.transactionCommit();
      Monitor.debug("FDSet update transaction committing on fdSet.id=" + fdSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("FDSet update transaction rollback on fdSet.id=" + fdSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update fdSet.id=" + fdSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Update the given fdSet in the database, including dependent objects, such as
   * fd profiles and fds.
   * Note the pre-existing dependents in the database are deleted first.
   * 
   * @see #write() if you want a transaction and logging around the operation.
   * 
   * @param fdSet  the fdSet
   */
  public void updateWithDependents(FDSet fdSet) throws DatabaseException {
    long fdSetID = fdSet.getLongId();

    deleteDependents(fdSetID);
    updateRow(fdSet);
    insertDependents(fdSet);
  }

  /**
   * Update just the fdSet row into the database. Ignores dependent objects.
   * 
   * @param fdSet  the fdSet
   */
  public void updateRow(FDSet fdSet) throws DatabaseException {
    String query = "update_fdSet_" + fdSet.getId();
    dbw.psCreate(query,
      "UPDATE VIA.FUND_DIAG_SETS SET NAME = ?, DESCRIPTION = ?, FUND_DIAG_TYPE = ? WHERE ID = ?"
    );
    // Note: do not update the project id. Must use separate API to move
    // this to a different project.
    
    try {
      dbw.psClearParams(query);

      dbw.psSetVarChar(query, 1,
        fdSet.getName() == null ? null : fdSet.getName().toString());
      
      dbw.psSetVarChar(query, 2,
        fdSet.getDescription() == null ? null : fdSet.getDescription().toString());

      dbw.psSetBigInt(query, 3,
        fdSet.getType() == null ? null : ((FDType)fdSet.getType()).getLongId());

      dbw.psSetBigInt(query, 4, fdSet.getLongId());
      
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "FDSet not unique: there exist " + rows + " with id=" + fdSet.getId(), dbw, query);
      }
    }
    finally {
      if (query != null) {
        dbw.psDestroy(query);
      }
    }
  }

  /**
   * Delete the given fdSet ID from the database, and all dependent rows.
   * 
   * @param fdSetID  the fdSet ID
   */
  public void delete(long fdSetID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      dbw.transactionBegin();
      Monitor.debug("FDSet delete transaction beginning on fdSet.id=" + fdSetID);
      
      deleteDependents(fdSetID);
      deleteRow(fdSetID);

      dbw.transactionCommit();
      Monitor.debug("FDSet delete transaction committing on fdSet.id=" + fdSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbw.transactionRollback();
        Monitor.debug("FDSet delete transaction rollback on fdSet.id=" + fdSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete fdSet.id=" + fdSetID, timeCommit - timeBegin);
  }

  /**
   * Delete just the fdSet row from the database. Ignores dependent objects.
   * 
   * @param fdSet  the fdSet
   */
  public void deleteRow(long fdSetID) throws DatabaseException {
    String query = "delete_fdSet_" + fdSetID;
    dbw.psCreate(query,
      "DELETE FROM VIA.FUND_DIAG_SETS WHERE ID = ?"
    );
    
    try {
      dbw.psClearParams(query);
      dbw.psSetBigInt(query, 1, fdSetID);
      
      long rows = dbw.psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "FDSet not unique: there exist " + rows + " with id=" + fdSetID, dbw, query);
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
   * @param fdSetID  the fdSet ID
   */
  private void deleteDependents(long fdSetID) throws DatabaseException {
    FDProfileWriter fdpWriter = new FDProfileWriter(dbParams, dbw);
    
    fdpWriter.deleteAllProfiles(fdSetID);
  }
}
