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

/**
 * Tests methods for writing Scenarios to a database.
 * @author vjoel
 */
public class ScenarioWriterTest {
  static ScenarioWriter scWriter;
  static ScenarioReader scReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    DBParams dbParams = new DBParams();
    
    scWriter = new ScenarioWriter(dbParams);
    scReader = new ScenarioReader(dbParams);
  }

  @Before
  public void setup() {
    // we assume scenario 99998 exists, but we could insert it here
    // we assume scenario 99997 does not exist, but we could delete it here
  }
  
  @Test
  public void testUpdateOneScenario() throws core.DatabaseException {
    Long scenarioID = 99998L;
    Scenario sc;
    
    sc = new Scenario();
    
    sc.setId(scenarioID);
    sc.setName("ScenarioWriterTest testUpdateOneScenario");
    sc.setDescription("for test");
    
    //System.out.println("Test Scenario: " + sc);
    
    scWriter.update(sc);
   
    Scenario sc2 = scReader.read(sc.getLongId());
    
    assertEquals(scenarioID, sc2.getLongId());
    assertEquals("for test", sc2.getDescription());
  }

  @Test
  public void testInsertDeleteOneScenario() throws core.DatabaseException {
    Long scenarioID = 99997L;
    Scenario sc;
    
    sc = new Scenario();
    
    sc.setId(scenarioID);
    sc.setName("ScenarioWriterTest testInsertDeleteOneScenario");
    
    //System.out.println("Test Scenario: " + sc);
    
    scWriter.insert(sc);
    
    Scenario sc2 = scReader.read(sc.getLongId());
    
    assertEquals(scenarioID, sc2.getLongId());
    
    scWriter.delete(scenarioID);
    
    Scenario sc3 = scReader.read(scenarioID);
    assertEquals(null, sc3);
  }
}
