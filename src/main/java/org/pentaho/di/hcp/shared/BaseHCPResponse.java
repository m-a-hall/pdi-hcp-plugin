/*******************************************************************************
 * Pentaho Data Integration
 *
 * <p/>
 * Copyright (c) 2002-2018 Hitachi Vantara. All rights reserved.
 * <p/>
 * ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/


package org.pentaho.di.hcp.shared;

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class BaseHCPResponse {

  public static final String RESPONSE_STATUS_TAG = "Status";
  public static final String X_HCP_TIME_TAG = "X-HCP-Time";
  public static final String SERVER_TAG = "Server";
  public static final String X_HCP_HASH_TAG = "X-HCP-Hash";
  public static final String X_HCP_SERVICED_BY_SYSTEM_TAG = "X-HCP-ServicedBySystem";
  public static final String X_REQUEST_ID_TAG = "X-RequestId";
  public static final String CONTENT_TYPE_TAG = "Content-Type";
  public static final String CONTENT_LENGTH_TAG = "Content-Length";

  protected Long m_contentLength;
  protected String m_contentType;
  protected Long m_time;
  protected String m_server;
  protected String m_hcpHash;
  protected Boolean m_success = false;
  protected String m_explanation;
  protected String m_requestID;
  protected String m_servicedBy;
  protected Status m_status;

  public BaseHCPResponse() {
  }

  public BaseHCPResponse( Status status, String explanation ) {
    m_explanation = explanation;
    m_status = status;
  }

  public BaseHCPResponse( Status status ) {

  }

  public void setContentLength( Long contentLength ) {
    m_contentLength = contentLength;
  }

  public Long getContentLength() {
    return m_contentLength;
  }

  public void setContentType( String contentType ) {
    m_contentType = contentType;
  }

  public String getContentType() {
    return m_contentType;
  }

  public void setTime( Long time ) {
    m_time = time;
  }

  public Long getTime() {
    return m_time;
  }

  public void setServer( String server ) {
    m_server = server;
  }

  public String getServer() {
    return m_server;
  }

  public void setHCPHash( String hashV ) {
    m_hcpHash = hashV;
  }

  public String getHCPHash() {
    return m_hcpHash;
  }

  public void setSuccess( Boolean success ) {
    m_success = success;
  }

  public Boolean getSuccess() {
    return m_success;
  }

  public void setExplanation( String explanation ) {
    m_explanation = explanation;
  }

  public String getExplanation() {
    return m_explanation;
  }

  public void setRequestID( String requestID ) {
    m_requestID = requestID;
  }

  public String getRequestID() {
    return m_requestID;
  }

  public void setServicedBy( String servicedBy ) {
    m_servicedBy = servicedBy;
  }

  public String getServicedBy() {
    return m_servicedBy;
  }

  public void setStatus( Status status ) {
    m_status = status;

    m_success = false;
    switch ( m_status ) {
      case OK:
      case CREATED:
      case PARTIAL_CONTENT:
      case NO_CONTENT:
        m_success = true;
        break;
      default:
        m_success = false;
    }
  }

  public Status getStatus() {
    return m_status;
  }

  public String toString() {
    StringBuilder b = new StringBuilder();
    if ( getStatus() == null ) {
      b.append( "" );
    } else {
      b.append( "Status: " ).append( getStatus().toString() ).append( "\n" );
    }

    return b.toString();
  }

  public static void addMinSysFieldMetadata( RowMetaInterface rowMeta ) throws KettlePluginException {
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( RESPONSE_STATUS_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_TIME_TAG, ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( SERVER_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_HASH_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta(
        ValueMetaFactory.createValueMeta( X_HCP_SERVICED_BY_SYSTEM_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_REQUEST_ID_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( CONTENT_TYPE_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( CONTENT_LENGTH_TAG, ValueMetaInterface.TYPE_INTEGER ) );
  }

  public void populateKettleRow( RowMetaInterface outputRowMeta, Object[] outputRow ) {
    int baseIndex = outputRowMeta.indexOfValue( RESPONSE_STATUS_TAG );
    if ( baseIndex >= 0 ) {
      outputRow[baseIndex++] = getStatus().toString();
      outputRow[baseIndex++] = getTime();
      outputRow[baseIndex++] = getServer();
      outputRow[baseIndex++] = getHCPHash();
      outputRow[baseIndex++] = getServicedBy();
      outputRow[baseIndex++] = getRequestID();
      outputRow[baseIndex++] = getContentType();
      outputRow[baseIndex] = getContentLength();
    }
  }

  public static enum Status {
    OK, CREATED, NO_CONTENT, PARTIAL_CONTENT, AUTH_MISSING, BAD_REQUEST, UNAUTHORIZED, FORBIDDEN, NOT_FOUND, CONFLICT, FILE_TOO_LARGE, REQUEST_TOO_LARGE, REQUEST_RANGE_INVALID, INTERNAL_ERROR, UNAVAILABLE, OTHER_FAILURE;
  }
}
