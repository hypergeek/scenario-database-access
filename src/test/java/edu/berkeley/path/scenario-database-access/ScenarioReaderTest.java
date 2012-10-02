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

import org.junit.*;
import static org.junit.Assert.*;

import edu.berkeley.path.model_elements.*;

import core.*;

/**
 * Tests methods for reading Scenarios from a database.
 * @author vjoel
 */
public class ScenarioReaderTest {
  static DatabaseReader dbr;
  
  @BeforeClass public static void dbsetup() throws DatabaseException {
    boolean usingOracle = true;
    
    String host = System.getenv("VIA_DATABASE_HOST");
    if (host == null) {
      host = "localhost";
    }
    
    String portstr = System.getenv("VIA_DATABASE_PORT");
    int port;
    if (portstr != null) {
      port = Integer.parseInt(portstr);
    }
    else {
      port = 21521;
    }
    
    String name = System.getenv("VIA_DATABASE_NAME");
    if (name == null) {
      name = "via";
    }
    
    String user = System.getenv("VIA_DATABASE_USER");
    if (user == null) {
      user = System.getProperty("user.name");
    }

    String pass = System.getenv("VIA_DATABASE_PASS");
    if (pass == null) {
      pass = "";
    }

    dbr = new DatabaseReader(
            usingOracle,
            host,
            port,
            name,
            user,
            pass);
  }

  @Before
  public void setup() {
    
  }
  
  @Test
  public void testReadOneScenario() { // throws IOException {
    System.out.println(dbr);
  }
}
