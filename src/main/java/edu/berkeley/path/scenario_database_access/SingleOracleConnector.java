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
}
