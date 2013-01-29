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
 * Encapsulates static storage of a single oracle connection, and the
 * methods to set up that connection. If you are using this class, you
 * should not normally call oraDatabase.doConnect() directly. Call the
 * methods of this class instead. This ensures that only one connection
 * is used.
 *
 * All methds and data are static, so it doesn't matter how often you
 * instantiate this class. Each instance will refer to the same state.
 * 
 * @author vjoel
 */
public class SingleOracleConnector {
  private static oraDatabase.dbConnectInfo connInfo = null;
  private static java.sql.Connection oraDB = null;
  private static DBParams dbParams = new DBParams();
  
  /**
   * Get the unique connection managed by this class. Normally,
   * there is no need to call this method. Just call executeSP.
   **/
  public static synchronized java.sql.Connection getConnection() {
    if (oraDB == null) {
      connInfo.uname = dbParams.user;
      connInfo.upass = dbParams.pass;
      connInfo.host = dbParams.host;
      connInfo.SID = dbParams.name;
      connInfo.port = dbParams.port;
      
      oraDB = oraDatabase.doConnect(connInfo);
    }
    return oraDB;
  }
  
  public static String getUser() {
    return dbParams.user;
  }
  
  public static int executeSP(String name, oraSPParams[] params) {
    return oraExecuteSP.callSP(getConnection(), name, params);
  }

  /**
   * Checks if this connection is currently within a transaction.
   * @return true if within a transaction, else return false.
   * @throws DatabaseException on any error.
   * @see #transactionBegin()
   * @see #transactionCommit()
   * @see #transactionRollback()
   */
  public static boolean transactionIsOpen() throws DatabaseException {
      try {
        return !getConnection().getAutoCommit();
      } catch (java.sql.SQLException sqlExp) {
        throw new DatabaseException(
                  null,
                  "Could not check if within a transaction.",
                  null,
                  null);
      }
  }

  /**
   * Start a transaction.  If commit is not later called, no changes will
   * be seen in the database.  This allows a large set of changes to be
   * written (or a consistent set read) to (from) the DB at one time
   * (atomic, ACID property).
   * <p/>
   * While in a transaction <u>any tables</u> touched (even ones that have
   * only been selected from) <b>will be locked</b> and any other queries
   * will be blocked until the transaction is committed or rolled-back.  So
   * try and keep what is done within a transaction as small as needed.
   * <p/>
   * For example if one is writings values to a set of links at a given time,
   * one can make it appear to other users that all the values appear at the
   * same time, and/or they <u>all</u> appear or <u>none</u> of them appear.
   * <p/>
   * This is also useful to get data from very large queries, see
   * {@link DatabaseReader#setFetchSize(int)}.
   * @throws DatabaseException on any error.
   * @see #transactionCommit()
   * @see #transactionRollback()
   */
  public static void transactionBegin() throws DatabaseException {
      if (transactionIsOpen()) {
          throw new DatabaseException(
                  null,
                  "Cannot begin a transaction while in another transaction.",
                  null,
                  null);
      }
      try {
          getConnection().setTransactionIsolation(
              java.sql.Connection.TRANSACTION_READ_COMMITTED);
          getConnection().setAutoCommit(false);
      } catch (java.sql.SQLException sqlExp) {
          throw new DatabaseException(
                  null,
                  "Could not start a transaction.",
                  null,
                  null);
      }
  }

  /**
   * Commits the transaction so all changes made in the transaction
   * are visible to others in one go.
   * @throws DatabaseException on any error.
   * @see #transactionBegin()
   * @see #transactionRollback()
   */
  public static void transactionCommit() throws DatabaseException {
      if (!transactionIsOpen()) {
          throw new DatabaseException(
                  null,
                  "Cannot commit outside of a transaction.",
                  null,
                  null);
      }
      try {
          getConnection().commit();
          getConnection().setAutoCommit(true);
      } catch (java.sql.SQLException sqlExp) {
          throw new DatabaseException(
                  null,
                  "Could not commit a transaction.",
                  null,
                  null);
      }
  }

  /**
   * Declare that any changes since the beginning of this transaction will
   * never be visible to other users.
   * @throws DatabaseException on any error.
   * @see #transactionBegin()
   * @see #transactionCommit()
   */
  public static void transactionRollback() throws DatabaseException {
      if (!transactionIsOpen()) {
          throw new DatabaseException(
                  null,
                  "Cannot rollback outside of a transaction.",
                  null,
                  null);
      }
      try {
          getConnection().rollback();
          getConnection().setAutoCommit(true);
      } catch (java.sql.SQLException sqlExp) {
          throw new DatabaseException(
                  null,
                  "Could not rollback a transaction.",
                  null,
                  null);
      }

  }
}
