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
 * Implements methods for reading Links from a database.
 * @see DBParams
 * @author vjoel
 */
public class LinkReader extends DatabaseReader {
  public LinkReader(
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
   * Read one link with the given ID from the database.
   * 
   * @param linkID    numerical ID of the link in the database
   * @param networkID numerical ID of the network
   * @return Link
   */
  public Link read(long linkID, long networkID) throws DatabaseException {
    Link link;
    String linkIdStr = "link.{id=" + linkID + ", network_id=" + networkID + "}";
    
    long timeBegin = System.nanoTime();
    
    try {
      transactionBegin();
      Monitor.debug("Link reader transaction beginning on " + linkIdStr);

      link = readWithAssociates(linkID, networkID);

      transactionCommit();
      Monitor.debug("Link reader transaction committing on " + linkIdStr);
    }
    catch (DatabaseException dbExc) {
      Monitor.err(dbExc);
      throw dbExc;
    }
    finally {
      try {
        transactionRollback();
        Monitor.debug("Link reader transaction rollback on " + linkIdStr);
      }
      catch(Exception Exc) {
        // Do nothing.
      }
    }
    
    long timeCommit = System.nanoTime();
    if (link != null) {
      Monitor.duration("Read " + linkIdStr, timeCommit - timeBegin);
    }

    return link;
  }

  /**
   * Read the link row with the given ID from the database.
   * 
   * @see #read() if you want a transaction and logging around the operation.
   * 
   * @param linkID  numerical ID of the link in the database
   * @param networkID numerical ID of the network
   * @return Link.
   */
  public Link readWithAssociates(long linkID, long networkID) throws DatabaseException {
    Link link = readRow(linkID, networkID);

    return link;
  }
  
  /**
   * Read the list of links associated with a network from the database.
   * This is intended to be called from @see NetworkReader, so it does
   * not set up a transaction of its own.
   * 
   * @param networkID numerical ID of the network
   * @return List of links.
   */
  public ArrayList<Link> readLinks(long networkID) throws DatabaseException {
    ArrayList<Link> links = new ArrayList<Link>();
    
    String query = null;
    Link link = null;
    
    try {
      query = runQueryAllLinks(networkID);
      while (null != (link = linkFromQueryRS(query))) {
        links.add(link);
      }
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
    
    return links;
  }

  /**
   * Read just the link row with the given ID from the database.
   * 
   * @param linkID  numerical ID of the link in the database
   * @param networkID numerical ID of the network
   * @return Link, with null for all dependent objects.
   */
  public Link readRow(long linkID, long networkID) throws DatabaseException {
    String query = null;
    Link link = null;
    
    try {
      query = runQueryOneLink(linkID, networkID);
      link = linkFromQueryRS(query);
    }
    finally {
      if (query != null) {
        psDestroy(query);
      }
    }
    
    return link;
  }

  /**
   * Execute a query for the specified link.
   * 
   * @param linkID  numerical ID of the link in the database
   * @param networkID numerical ID of the network
   * @return String     query string, may be passed to psRSNext or linkFromQueryRS
   */
  protected String runQueryOneLink(long linkID, long networkID) throws DatabaseException {
    String query = "read_link_" + linkID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"LINKS\" WHERE ((\"ID\" = ?) AND (\"NETWORK_ID\" = ?))"
    ); // TODO reuse this
    
    psClearParams(query);
    psSetBigInt(query, 1, linkID);
    psSetBigInt(query, 2, networkID);
    psQuery(query);

    return query;
  }

  /**
   * Execute a query for all links in specified network.
   * 
   * @param linkID  numerical ID of the link in the database
   * @param networkID numerical ID of the network
   * @return String     query string, may be passed to psRSNext or linkFromQueryRS
   */
  protected String runQueryAllLinks(long networkID) throws DatabaseException {
    String query = "read_links_network" + networkID;
    
    psCreate(query,
      "SELECT * FROM \"VIA\".\"LINKS\" WHERE (\"NETWORK_ID\" = ?)"
    );
    
    psClearParams(query);
    psSetBigInt(query, 1, networkID);
    psQuery(query);

    return query;
  }

  /**
   * Instantiate and populate a link object from the result set
   * of a link query.
   * 
   * @param query string
   * @return Link
   */
  protected Link linkFromQueryRS(String query) throws DatabaseException {
    Link link = null;
    
    if (psRSNext(query)) {
      //String columns = org.apache.commons.lang.StringUtils.join(psRSColumnNames(query), ", ");
      //System.out.println("columns: [" + columns + "]");
      
      link = new Link();
      
      Long id = psRSGetBigInt(query, "ID");
      Long bId = psRSGetBigInt(query, "BEG_NODE_ID");
      Long eId = psRSGetBigInt(query, "END_NODE_ID");
      Integer speed = psRSGetInteger(query, "SPEED_LIMIT");
      Double length = psRSGetDouble(query, "LENGTH");
      Integer detail = psRSGetInteger(query, "DETAIL_LEVEL");
      
// TODO go to the link_names table for this
//      String name = psRSGetVarChar(query, "NAME");
// TODO where is this now?
//      String type = psRSGetVarChar(query, "TYPE");
      
      link.setId(id);
//      link.name = name;
//      link.type = type;
      link.setBeginId(bId.toString());
      link.setEndId(eId.toString());
      link.setSpeedLimit(speed);
      link.setDetailLevel(detail);
      link.setLength(length);
//detail

      //System.out.println("Link: " + link);
    }

    return link;
  }
}
