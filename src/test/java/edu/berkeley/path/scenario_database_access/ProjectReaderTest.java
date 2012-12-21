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

import java.util.*;

/**
 * Tests methods for reading Projects from a database.
 * @author vjoel
 */
public class ProjectReaderTest {
  static ProjectReader prjReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    prjReader = new ProjectReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume project 99999 exists, but we could insert it here
    // we assume scenarios 99999, 99996 exist and are in project 99999
  }
  
  @Test
  public void testReadOneProject() throws core.DatabaseException {
    Long projectID = 99999L;
    Project prj;
    
    prj = prjReader.read(projectID);

    //System.out.println("Test Project: " + prj);
    
    assertTrue(null != prj);
    assertEquals(projectID, prj.getLongId());
  }
  
  @Test
  public void testReadScenarioList() throws core.DatabaseException {
    Long projectID = 99999L;
    
    List<Scenario> scenarios = prjReader.readScenarios(projectID);
    
    //System.out.println("Scenarios in project: " + projectID);
    //for (Scenario sc : scenarios) {
    //  System.out.println(sc);
    //}

    assertEquals(2, scenarios.size());
  }
  
  @Test
  public void testReadProjects() throws core.DatabaseException {
    List<Project> projects = prjReader.readProjects();
    //System.out.println(projects);
    //[{"id": "1", "name": "test", "description": "This is a test project"}, {"id": "99999", "name": "reader test", "description": null}]

    assertTrue(2 <= projects.size());
  }
}
