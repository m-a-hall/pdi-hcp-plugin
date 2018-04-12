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
public class HCPReadResponse extends BaseHCPResponse {

  public static final String X_HCP_TYPE_TAG = "X-HCP-Type";
  public static final String X_HCP_SIZE_TAG = "X-HCP-Size";
  public static final String X_HCP_VERSION_ID_TAG = "X-HCP-VersionId";
  public static final String X_HCP_INGEST_TIME_TAG = "X-HCP-IngestTime";
  public static final String X_HCP_RETENTION_CLASS_TAG = "X-HCP-RetentionClass";
  public static final String X_HCP_RETENTION_STRING_TAG = "X-HCP-RetentionString";
  public static final String X_HCP_RETENTION_TAG = "X-HCP-Retention";
  public static final String X_HCP_RETENTION_HOLD_TAG = "X-HCP-RetentionHold";
  public static final String X_HCP_SHRED_TAG = "X-HCP-Shred";
  public static final String X_HCP_DPL_TAG = "X-HCP-DPL";
  public static final String X_HCP_INDEX_TAG = "X-HCP-Index";
  public static final String X_HCP_CUSTOM_METADATA_TAG = "X-HCP-Custom-Metadata";
  public static final String X_HCP_CUSTOM_METADATA_ANNOTATIONS_TAG = "X-HCP-CustomMetadataAnnotations";
  public static final String X_HCP_OWNER_TAG = "X-HCP-Owner";
  public static final String X_HCP_DOMAIN_TAG = "X-HCP-Domain";
  public static final String X_HCP_REPLICATED_TAG = "X-HCP-Replicated";
  public static final String X_HCP_REPLICATION_COLLISION_TAG = "X-HCP-ReplicationCollision";
  public static final String X_HCP_CHANGE_TIME_STRING_TAG = "X-HCP-ChangeTimeString";

  protected String m_type;
  protected Long m_size;
  protected String m_versionId;
  protected Long m_ingestTime;
  protected String m_retentionClass;
  protected String m_retentionString;
  protected Long m_retention;
  protected Boolean m_hold;
  protected Boolean m_shred;
  protected Long m_dpl;
  protected Boolean m_indexed;
  protected Boolean m_customMetadata;
  protected String m_customMetadataAnnotations;
  protected String m_owner;
  protected String m_domain;
  protected Boolean m_replicated;
  protected Boolean m_replicationCollision;
  protected String m_changeTimeString;

  public HCPReadResponse() {
  }

  public HCPReadResponse( Status status, String explanation ) {
    super( status, explanation );
  }

  public Boolean getCustomMetadata() {
    return m_customMetadata;
  }

  public void setCustomMetadata( Boolean customMetadata ) {
    m_customMetadata = customMetadata;
  }

  public void setCustomMetadataAnnotations( String customMetadataAnnotations ) {
    m_customMetadataAnnotations = customMetadataAnnotations;
  }

  public String getCustomMetadataAnnotations() {
    return m_customMetadataAnnotations;
  }

  public Long getDpl() {
    return m_dpl;
  }

  public void setDpl( Long dpl ) {
    m_dpl = dpl;
  }

  public Boolean isIndexed() {
    return m_indexed;
  }

  public void setIndexed( Boolean indexed ) {
    m_indexed = indexed;
  }

  public void setIngestTime( Long ingestTime ) {
    m_ingestTime = ingestTime;
  }

  public Long getIngestTime() {
    return m_ingestTime;
  }

  public Boolean getIndexed() {
    return m_indexed;
  }

  public Long getRetention() {
    return m_retention;
  }

  public void setRetention( Long retention ) {
    m_retention = retention;
  }

  public String getRetentionClass() {
    return m_retentionClass;
  }

  public void setRetentionClass( String retentionClass ) {
    m_retentionClass = retentionClass;
  }

  public Boolean getRetentionHold() {
    return m_hold;
  }

  public void setRetentionHold( Boolean retentionHold ) {
    m_hold = retentionHold;
  }

  public String getRetentionString() {
    return m_retentionString;
  }

  public void setRetentionString( String retentionString ) {
    m_retentionString = retentionString;
  }

  public Boolean getShred() {
    return m_shred;
  }

  public void setShred( Boolean shred ) {
    m_shred = shred;
  }

  public Long getSize() {
    return m_size;
  }

  public void setSize( Long size ) {
    m_size = size;
  }

  public String getType() {
    return m_type;
  }

  public void setType( String type ) {
    m_type = type;
  }

  public String getVersionId() {
    return m_versionId;
  }

  public void setVersionId( String versionId ) {
    m_versionId = versionId;
  }

  public void setOwner( String owner ) {
    m_owner = owner;
  }

  public String getOwner() {
    return m_owner;
  }

  public void setDomain( String domain ) {
    m_domain = domain;
  }

  public String getDomain() {
    return m_domain;
  }

  public void setReplicated(Boolean replicated) {
    m_replicated = replicated;
  }

  public Boolean getReplicated() {
    return m_replicated;
  }

  public void setReplicationCollision(Boolean replicationCollision) {
    m_replicationCollision = replicationCollision;
  }

  public Boolean getReplicationCollision() {
    return m_replicationCollision;
  }

  public void setChangeTimeString(String changeTimeString) {
    m_changeTimeString = changeTimeString;
  }

  public String getChangeTimeString() {
    return m_changeTimeString;
  }

  public static void addReadFieldMetadata( RowMetaInterface rowMeta ) throws KettlePluginException {
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_TYPE_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_SIZE_TAG, ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_VERSION_ID_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_INGEST_TIME_TAG, ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta
        .addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_RETENTION_CLASS_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta
        .addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_RETENTION_STRING_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_RETENTION_TAG, ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta
        .addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_RETENTION_HOLD_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_SHRED_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_DPL_TAG, ValueMetaInterface.TYPE_INTEGER ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_INDEX_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta
        .addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_CUSTOM_METADATA_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta.addValueMeta(
        ValueMetaFactory.createValueMeta( X_HCP_CUSTOM_METADATA_ANNOTATIONS_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_OWNER_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_DOMAIN_TAG, ValueMetaInterface.TYPE_STRING ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_REPLICATED_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta.addValueMeta(
        ValueMetaFactory.createValueMeta( X_HCP_REPLICATION_COLLISION_TAG, ValueMetaInterface.TYPE_BOOLEAN ) );
    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( X_HCP_CHANGE_TIME_STRING_TAG, ValueMetaInterface.TYPE_STRING ) );
  }

  @Override public void populateKettleRow( RowMetaInterface outputRowMeta, Object[] outputRow ) {
    super.populateKettleRow( outputRowMeta, outputRow );
    int baseIndex = outputRowMeta.indexOfValue( X_HCP_TYPE_TAG );
    if ( baseIndex >= 0 ) {
      outputRow[baseIndex++] = getType();
      outputRow[baseIndex++] = getSize();
      outputRow[baseIndex++] = getVersionId();
      outputRow[baseIndex++] = getIngestTime();
      outputRow[baseIndex++] = getRetentionClass();
      outputRow[baseIndex++] = getRetentionString();
      outputRow[baseIndex++] = getRetention();
      outputRow[baseIndex++] = getRetentionHold();
      outputRow[baseIndex++] = getShred();
      outputRow[baseIndex++] = getDpl();
      outputRow[baseIndex++] = getIndexed();
      outputRow[baseIndex++] = getCustomMetadata();
      outputRow[baseIndex++] = getCustomMetadataAnnotations();
      outputRow[baseIndex++] = getOwner();
      outputRow[baseIndex++] = getDomain();
      outputRow[baseIndex++] = getReplicated();
      outputRow[baseIndex++] = getReplicationCollision();
      outputRow[baseIndex] = getChangeTimeString();
    }
  }
}
