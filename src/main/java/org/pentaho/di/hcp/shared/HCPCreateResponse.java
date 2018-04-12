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
import org.pentaho.di.hcp.shared.BaseHCPResponse;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPCreateResponse extends BaseHCPResponse {

  public static final String X_ARC_HASH_TAG = "X-ArcHash";
  public static final String LOCATION_TAG = "Location";
  public static final String X_ARC_CLUSTER_TIME_TAG = "X-ArcClusterTime";
  public static final String X_HCP_VERSION_ID_TAG = "X-HCP-VersionId";

  protected String m_location;
  protected String m_arcHash;
  protected Long m_clusterTime = -1L;
  protected String m_versionID;

  public HCPCreateResponse() {

  }

  public HCPCreateResponse( Status status, String explanation ) {
    super( status, explanation );
  }

  public void setLocation( String location ) {
    m_location = location;
  }

  public String getLocation() {
    return m_location;
  }

  public void setArcHash( String hash ) {
    m_arcHash = hash;
  }

  public String getArcHash() {
    return m_arcHash;
  }

  public void setClusterTime( Long clusterTime ) {
    m_clusterTime = clusterTime;
  }

  public Long getClusterTime() {
    return m_clusterTime;
  }

  public void setVersionID( String versionID ) {
    m_versionID = versionID;
  }

  public String getVersionID() {
    return m_versionID;
  }

  public static void addCreateFieldMetadata( RowMetaInterface rowMeta ) throws KettlePluginException {
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( HCPCreateResponse.LOCATION_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( HCPCreateResponse.X_ARC_CLUSTER_TIME_TAG, ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( HCPCreateResponse.X_HCP_VERSION_ID_TAG, ValueMetaInterface.TYPE_STRING ) );
  }

  @Override
  public void populateKettleRow( RowMetaInterface outputRowMeta, Object[] outputRow ) {
    super.populateKettleRow( outputRowMeta, outputRow );
    int baseIndex = outputRowMeta.indexOfValue( LOCATION_TAG );
    if ( baseIndex >= 0 ) {
      outputRow[baseIndex++] = getLocation();
      outputRow[baseIndex++] = getClusterTime();
      outputRow[baseIndex] = getVersionID();
    }
  }

  public HCPCreateResponse( Status status, String location, String hash, Long clusterTime, String versionID ) {
    this( status, "" );
    m_arcHash = hash;
    m_clusterTime = clusterTime;
    m_versionID = versionID;
    m_location = location;
  }
}
