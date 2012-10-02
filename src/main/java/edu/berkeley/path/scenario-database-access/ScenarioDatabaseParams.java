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

/**
 * Captures parameters for accessing Scenarios from a database.
 * 
 * Roughly equivalent to a connection string, but with accessibe fields.
 * The fields correspond exactly to the constructor of a core.Database.
 * The param object is used in the constructor of a ScenarioReader or
 * ScenarioWriter.
 * 
 * @author vjoel
 */
public class ScenarioDatabaseParams {
  public boolean usingOracle = true;
  public String host;
  public int port;
  public String name;
  public String user;
  public String pass;

  public ScenarioDatabaseParams() {
    this.host = System.getenv("VIA_DATABASE_HOST");
    if (this.host == null) {
      this.host = "localhost";
    }
    
    String portstr = System.getenv("VIA_DATABASE_PORT");
    if (portstr != null) {
      this.port = Integer.parseInt(portstr);
    }
    else {
      this.port = 1521;
    }
    
    this.name = System.getenv("VIA_DATABASE_NAME");
    if (this.name == null) {
      this.name = "via";
    }
    
    this.user = System.getenv("VIA_DATABASE_USER");
    if (this.user == null) {
      this.user = System.getProperty("user.name");
    }

    this.pass = System.getenv("VIA_DATABASE_PASS");
    if (this.pass == null) {
      this.pass = "";
    }
  }
}
