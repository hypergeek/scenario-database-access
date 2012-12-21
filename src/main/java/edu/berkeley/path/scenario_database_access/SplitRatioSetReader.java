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
 * Implements methods for reading SplitRatioSets from a database.
 * @see DBParams
 * @author vjoel
 */
public class SplitRatioSetReader extends ReaderBase {
  public SplitRatioSetReader(
          DBParams dbParams
          ) throws DatabaseException {
    super(dbParams);
  }
  
  public SplitRatioSetReader(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    super(dbParams, dbReader);
  }
  
  /**
   * Read one split ratio set with the given ID from the database, plus
   * all dependent objects, namely the split ratio profiles and their
   * dependent maps of arrays.
   * 
   * @param splitratioSetID  ID of the Set in the database
   * @return SplitRatioSet
   */
  public SplitRatioSet read(long splitratioSetID) throws DatabaseException {
    SplitRatioSet splitratioSet;
    
    long timeBegin = System.nanoTime();
    
    try {
      dbr.transactionBegin();
      Monitor.debug(
        "SplitRatioSet reader transaction beginning on splitratioSet.id=" +
        splitratioSetID);

      splitratioSet = readWithDependents(splitratioSetID);

      dbr.transactionCommit();
      Monitor.debug(
        "SplitRatioSet reader transaction committing on splitratioSet.id=" +
        splitratioSetID);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        dbr.transactionRollback();
        Monitor.debug(
          "SplitRatioSet reader transaction rollback on splitratioSet.id=" +
          splitratioSetID);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (splitratioSet != null) {
      Monitor.duration(
        "Read splitratioSet.id=" +
        splitratioSet.getId(), timeCommit - timeBegin);
    }

    return splitratioSet;
  }

  /**
   * Read the SplitRatioSet row with the given ID from the database, plus
   * all dependent objects, namely the split ratio profiles and their
   * dependent maps of arrays.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param splitratioSetID  ID of the splitratioSet in the database
   * @return SplitRatioSet.
   */
  public SplitRatioSet readWithDependents(long splitratioSetID) throws DatabaseException {
    SplitRatioSet splitratioSet = readRow(splitratioSetID);

    if (splitratioSet != null) {
      SplitRatioProfileReader srpReader = new SplitRatioProfileReader(dbParams, dbr);
      splitratioSet.setProfileMap(srpReader.readProfiles(splitratioSetID));
    }
    
    return splitratioSet;
  }

  /**
   * Read just the splitratioSet row with the given ID from the database. Ignores
   * dependent objects.
   * 
   * @param splitratioSetID  ID of the splitratioSet in the database
   * @return SplitRatioSet, with null for all dependent objects.
   */
  public SplitRatioSet readRow(long splitratioSetID) throws DatabaseException {
    String query = null;
    SplitRatioSet splitratioSet = null;
    
    try {
      query = runQuery(splitratioSetID);
      splitratioSet = splitratioSetFromQueryRS(query);
    }
    finally {
      if (query != null) {
        dbr.psDestroy(query);
      }
    }
    
    return splitratioSet;
  }

  /**
   * Execute a query for the specified splitratioSet.
   * 
   * @param splitratioSetID  ID of the splitratioSet in the database
   * @return String     query string, may be passed to psRSNext or splitratioSetFromQueryRS
   */
  protected String runQuery(long splitratioSetID) throws DatabaseException {
    String query = "read_splitratioSet_" + splitratioSetID;
    
    dbr.psCreate(query,
      "SELECT * FROM VIA.SPLIT_RATIO_SETS WHERE (ID = ?)"
    );
    
    dbr.psClearParams(query);
    dbr.psSetBigInt(query, 1, splitratioSetID);
    dbr.psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a splitratioSet object from the result set
   * of a splitratioSet query. Do not attempt to read dependent rows.
   * 
   * @param query string
   * @return SplitRatioSet
   */
  protected SplitRatioSet splitratioSetFromQueryRS(String query) throws DatabaseException {
    SplitRatioSet splitratioSet = null;
    
    while (dbr.psRSNext(query)) {
      if (splitratioSet != null) {
        throw new DatabaseException(null,
          "SplitRatioSet not unique: " + query, dbr, query);
      }
      
      //String columns = org.apache.commons.lang.StringUtils.join(dbr.psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      splitratioSet = new SplitRatioSet();
      
      Long id = dbr.psRSGetBigInt(query, "ID");
      String name = dbr.psRSGetVarChar(query, "NAME");
      String desc = dbr.psRSGetVarChar(query, "DESCRIPTION");
      Long prjId = dbr.psRSGetBigInt(query, "PROJECT_ID");
      
      splitratioSet.setId(id.toString());
      splitratioSet.name = name;
      splitratioSet.description = desc;
      splitratioSet.setProjectId(prjId == null ? null : prjId.toString());

      //System.out.println("SplitRatioSet: " + splitratioSet);
    }

    return splitratioSet;
  }
}
