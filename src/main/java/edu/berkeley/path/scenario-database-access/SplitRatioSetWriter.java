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
 * Implements methods for writing SplitRatioSets to a database.
 * @see DBParams
 * @author vjoel
 */
public class SplitRatioSetWriter extends DatabaseWriter {
  public SplitRatioSetWriter(
          DBParams dbParams
          ) throws DatabaseException {
    super(
      dbParams.usingOracle,
      dbParams.host,
      dbParams.port,
      dbParams.name,
      dbParams.user,
      dbParams.pass);
    this.dbParams = dbParams;
  }
  
  DBParams dbParams;
  
  /**
   * Insert the given split ratio set into the database.
   * 
   * @param splitratioSet  the split ratio set
   */
  public void insert(SplitRatioSet splitratioSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
//      transactionBegin();
      Monitor.debug("SplitRatioSet insert transaction beginning on splitratioSet.id=" + splitratioSet.getId());
      
      insertWithDependents(splitratioSet);

      Monitor.debug("SplitRatioSet insert transaction committing on splitratioSet.id=" + splitratioSet.getId());
//      transactionCommit();
      Monitor.debug("SplitRatioSet insert transaction committed on splitratioSet.id=" + splitratioSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
//        transactionRollback();
        Monitor.debug("SplitRatioSet insert transaction rollback on splitratioSet.id=" + splitratioSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Insert splitratioSet.id=" + splitratioSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Insert the given splitratioSet into the database, including dependent objects.
   * 
   * @param splitratioSet  the splitratioSet
   */
  public void insertWithDependents(SplitRatioSet splitratioSet) throws DatabaseException {
    insertRow(splitratioSet);
    insertDependents(splitratioSet);
  }
  
  private void insertDependents(SplitRatioSet splitratioSet) throws DatabaseException {
    SplitRatioProfileWriter srpWriter = new SplitRatioProfileWriter(this.dbParams);
    long splitratioSetID = splitratioSet.getLongId();
    
    srpWriter.insertProfiles(splitratioSet.getProfileMap(), splitratioSetID);
  }

  /**
   * Insert just the splitratioSet row into the database. Ignores dependent objects.
   * 
   * @param splitratioSet  the splitratioSet
   */
  public void insertRow(SplitRatioSet splitratioSet) throws DatabaseException {
    String query = "insert_splitratioSet_" + splitratioSet.getId();
    psCreate(query,
      "INSERT INTO \"VIA\".\"SPLIT_RATIO_SETS\" (ID, NAME, DESCRIPTION) VALUES(?, ?, ?)"
    );
  
    try {
      psClearParams(query);

      psSetBigInt(query, 1, splitratioSet.getLongId());
      
      psSetVarChar(query, 2,
        splitratioSet.getName() == null ? null : splitratioSet.getName().toString());
      
      psSetVarChar(query, 3,
        splitratioSet.getDescription() == null ? null : splitratioSet.getDescription().toString());
      
      psUpdate(query);
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }
  
  /**
   * Update the given splitratioSet in the database.
   * 
   * @param splitratioSet  the splitratioSet
   */
  public void update(SplitRatioSet splitratioSet) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("SplitRatioSet update transaction beginning on splitratioSet.id=" + splitratioSet.getId());
      
      updateWithDependents(splitratioSet);

      transactionCommit();
      Monitor.debug("SplitRatioSet update transaction committing on splitratioSet.id=" + splitratioSet.getId());
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("SplitRatioSet update transaction rollback on splitratioSet.id=" + splitratioSet.getId());
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Update splitratioSet.id=" + splitratioSet.getId(), timeCommit - timeBegin);
  }

  /**
   * Update the given splitratioSet in the database, including dependent objects, such as
   * splitratio profiles and splitratios.
   * Note the pre-existing dependents in the database are deleted first.
   * 
   * @see #write() if you want a transaction and logging around the operation.
   * 
   * @param splitratioSet  the splitratioSet
   */
  public void updateWithDependents(SplitRatioSet splitratioSet) throws DatabaseException {
    long splitratioSetID = splitratioSet.getLongId();

    deleteDependents(splitratioSetID);
    updateRow(splitratioSet);
    insertDependents(splitratioSet);
  }

  /**
   * Update just the splitratioSet row into the database. Ignores dependent objects.
   * 
   * @param splitratioSet  the splitratioSet
   */
  public void updateRow(SplitRatioSet splitratioSet) throws DatabaseException {
    String query = "update_splitratioSet_" + splitratioSet.getId();
    psCreate(query,
      "UPDATE \"VIA\".\"SPLIT_RATIO_SETS\" SET \"NAME\" = ?, \"DESCRIPTION\" = ? WHERE \"ID\" = ?"
    );
    
    try {
      psClearParams(query);

      psSetVarChar(query, 1,
        splitratioSet.getName() == null ? null : splitratioSet.getName().toString());
      
      psSetVarChar(query, 2,
        splitratioSet.getDescription() == null ? null : splitratioSet.getDescription().toString());

      psSetBigInt(query, 3, splitratioSet.getLongId());
      
      long rows = psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "SplitRatioSet not unique: there exist " + rows + " with id=" + splitratioSet.getId(), this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }

  /**
   * Delete the given splitratioSet ID from the database, and all dependent rows.
   * 
   * @param splitratioSetID  the splitratioSet ID
   */
  public void delete(long splitratioSetID) throws DatabaseException {
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("SplitRatioSet delete transaction beginning on splitratioSet.id=" + splitratioSetID);
      
      deleteDependents(splitratioSetID);
      deleteRow(splitratioSetID);

      transactionCommit();
      Monitor.debug("SplitRatioSet delete transaction committing on splitratioSet.id=" + splitratioSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("SplitRatioSet delete transaction rollback on splitratioSet.id=" + splitratioSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }

    long timeCommit = System.nanoTime();
    Monitor.duration("Delete splitratioSet.id=" + splitratioSetID, timeCommit - timeBegin);
  }

  /**
   * Delete just the splitratioSet row from the database. Ignores dependent objects.
   * 
   * @param splitratioSet  the splitratioSet
   */
  public void deleteRow(long splitratioSetID) throws DatabaseException {
    String query = "delete_splitratioSet_" + splitratioSetID;
    psCreate(query,
      "DELETE FROM \"VIA\".\"SPLIT_RATIO_SETS\" WHERE \"ID\" = ?"
    );
    
    try {
      psClearParams(query);
      psSetBigInt(query, 1, splitratioSetID);
      
      long rows = psUpdate(query);
      
      if (rows != 1) {
        throw new DatabaseException(null, "SplitRatioSet not unique: there exist " + rows + " with id=" + splitratioSetID, this, query);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
  }

  /**
   * Delete just the dependent objects.
   * 
   * @param splitratioSetID  the splitratioSet ID
   */
  private void deleteDependents(long splitratioSetID) throws DatabaseException {
    SplitRatioProfileWriter srpWriter = new SplitRatioProfileWriter(this.dbParams);
    
    srpWriter.deleteAllProfiles(splitratioSetID);
  }
}
