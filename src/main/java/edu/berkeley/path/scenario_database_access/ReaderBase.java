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

import core.*;

/**
 * Base class for all classes that read from a database.
 * Can be constructed with or without a DatabaseReader.
 * In the former case, the dbParams are used to construct a reader.
 * The latter case is for passing a reader down the containment
 * hierarchy (such as from DemandSetReader to DemandProfileReader), so
 * that all instances in the hierarchy use the same underlying connection.
 * 
 * @see DBParams
 * @author vjoel
 */
public class ReaderBase {
  protected DatabaseReader dbr;
  protected DBParams dbParams;
  
  /**
   * Create a reader base with a new connection to the db,
   * specified by the dbParams.
   **/
  public ReaderBase(
          DBParams dbParams
          ) throws DatabaseException {
    this.dbParams = dbParams;
    this.dbr = new DatabaseReader(
      dbParams.usingOracle,
      dbParams.host,
      dbParams.port,
      dbParams.name,
      dbParams.user,
      dbParams.pass);
  }
  
  /**
   * Create a reader base reusing a given connection to the db,
   * specified by the dbReader.
   **/
  public ReaderBase(
          DBParams dbParams,
          DatabaseReader dbReader
          ) throws DatabaseException {
    this.dbParams = dbParams;
    this.dbr = dbReader;
  }
  
  public DBParams getDBParams() {
    return dbParams;
  }
  
  public DatabaseReader getDatabaseReader() {
    return dbr;
  }
}
