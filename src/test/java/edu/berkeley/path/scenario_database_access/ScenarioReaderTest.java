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
 * Tests methods for reading Scenarios from a database.
 * @author vjoel
 */
public class ScenarioReaderTest {
  static ScenarioReader scReader;
  
  @BeforeClass public static void dbsetup() throws core.DatabaseException {
    scReader = new ScenarioReader(new DBParams());
  }

  @Before
  public void setup() {
    // we assume scenario 99999 exists, but we could insert it here
    // we assume network 99989 exists, but we could insert it here
    // we assume split_ratio_set 99999 exists, but we could insert it here
    // we assume demand_set 99999 exists, but we could insert it here
    // we assume fd_set 99999 exists, but we could insert it here
    // we assume sensor_set 99999 exists, but we could insert it here
  }
  
  @Test
  public void testReadOneScenario() throws core.DatabaseException {
    Long scenarioID = 99999L;
    Scenario sc;
    
    sc = scReader.read(scenarioID);

    //System.out.println("Test Scenario: " + sc);
    
    assertEquals(scenarioID, sc.getLongId());
    
    List<Network> networks = sc.getNetworkList();
    assertEquals(1, networks.size());
    
    assertEquals(2, networks.get(0).getNodeList().size());
    assertEquals(1, networks.get(0).getLinkList().size());
    
    assertTrue(null != sc.getSplitratioSet());
    assertTrue(null != sc.getFdSet());
    assertTrue(null != sc.getDemandSet());
    assertTrue(null != sc.getSensorSet());
  }
}
