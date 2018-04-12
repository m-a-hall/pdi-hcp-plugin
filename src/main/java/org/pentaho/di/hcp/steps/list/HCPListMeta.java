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

package org.pentaho.di.hcp.steps.list;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.hcp.shared.BaseHCPResponse;
import org.pentaho.di.hcp.shared.HCPConnection;
import org.pentaho.di.hcp.shared.HCPConnectionUtils;
import org.pentaho.di.hcp.shared.HCPListResponse;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Node;

import java.util.List;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
@Step( id = "HCPList", name = "HCP List", description = "Hitachi Content Platform : this step allows you to list documents in a folder on the store", categoryDescription = "HCP", image = "ui/images/FRC.svg" )
public class HCPListMeta extends BaseStepMeta implements StepMetaInterface {

  protected static Class<?> PKG = HCPListMeta.class;

  protected static final String TAG_CONNECTION = "connection";
  protected static final String TAG_SOURCE_FILE = "source_field";
  protected static final String TAG_TARGET_FILE = "target_field";

  public static final String RESPONSE_TIME_FIELD_NAME = "Elapsed time ms";

  protected HCPConnection m_connection;

  protected String m_sourceFileField;

  public HCPListMeta() {
    super();
  }

  @Override public void setDefault() {
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new HCPList( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new HCPListData();
  }

  @Override public String getDialogClassName() {
    return HCPListDialog.class.getName();
  }

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    ValueMetaInterface timeValue = new ValueMetaInteger( RESPONSE_TIME_FIELD_NAME );
    timeValue.setLength( 7 );
    timeValue.setOrigin( name );
    inputRowMeta.addValueMeta( timeValue );

    try {
      BaseHCPResponse.addMinSysFieldMetadata( inputRowMeta );
      HCPListResponse.addListFieldMetadata( inputRowMeta );
    } catch ( KettlePluginException e ) {
      throw new KettleStepException( e );
    }
  }

  @Override public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.addTagValue( TAG_CONNECTION, m_connection == null ? null : m_connection.getName() ) );
    xml.append( XMLHandler.addTagValue( TAG_SOURCE_FILE, m_sourceFileField ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
      throws KettleXMLException {
    try {

      String connectionName = XMLHandler.getTagValue( stepnode, TAG_CONNECTION );
      if ( StringUtils.isNotEmpty( connectionName ) ) {
        try {
          m_connection = HCPConnectionUtils.getConnectionFactory( metaStore ).loadElement( connectionName );
        } catch ( MetaStoreException e ) {
          // We just log the message but we don't abort the complete meta-data
          // loading.
          //
          log.logError( BaseMessages.getString( PKG, "HCPListMeta.Error.HCPConnectionNotFound", connectionName ) );
          m_connection = null;
        }
      }
      m_sourceFileField = XMLHandler.getTagValue( stepnode, TAG_SOURCE_FILE );
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "HCPListMeta.Error.CouldNotLoadXML" ), e );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
      throws KettleException {

    rep.saveStepAttribute( id_transformation, id_step, TAG_CONNECTION,
        m_connection == null ? null : m_connection.getName() );
    rep.saveStepAttribute( id_transformation, id_step, TAG_SOURCE_FILE, m_sourceFileField );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
      throws KettleException {

    String connectionName = rep.getStepAttributeString( id_step, TAG_CONNECTION );
    if ( StringUtils.isNotEmpty( connectionName ) ) {
      try {
        m_connection = HCPConnectionUtils.getConnectionFactory( metaStore ).loadElement( connectionName );
      } catch ( MetaStoreException e ) {
        // We just log the message but we don't abort the complete meta-data
        // loading.
        //
        log.logError( BaseMessages.getString( PKG, "HCPListMeta.Error.HCPConnectionNotFound", connectionName ) );
        m_connection = null;
      }
    }
    m_sourceFileField = rep.getStepAttributeString( id_step, TAG_SOURCE_FILE );
  }

  public HCPConnection getConnection() {
    return m_connection;
  }

  public void setConnection( HCPConnection connection ) {
    m_connection = connection;
  }

  public String getSourceFileField() {
    return m_sourceFileField;
  }

  public void setSourceFileField( String sourceFileField ) {
    m_sourceFileField = sourceFileField;
  }

  @Override public boolean supportsErrorHandling() {
    return true;
  }
}
