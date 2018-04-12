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

import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.hcp.shared.HCPConnectionOperationUtils;
import org.pentaho.di.hcp.shared.HCPListResponse;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.List;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPList extends BaseStep implements StepInterface {

  public HCPList( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    HCPListMeta meta = (HCPListMeta) smi;
    HCPListData data = (HCPListData) sdi;

    boolean error = false;
    if ( meta.getConnection() == null ) {
      log.logError( BaseMessages.getString( HCPListMeta.PKG, "HCPList.Error.HCPConnectionNotSpecified" ) );
      error = true;
    }
    if ( StringUtils.isEmpty( meta.getSourceFileField() ) ) {
      log.logError( BaseMessages.getString( HCPListMeta.PKG, "HCPList.Error.SourceFileFieldNotSpecified" ) );
      error = true;
    }

    if ( error ) {
      // Stop right here.
      return false;
    }

    data.authorization = meta.getConnection().getAuthorizationHeader();
    data.client = ApacheHttpClient.create( new DefaultApacheHttpClientConfig() );

    return super.init( smi, sdi );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    HCPListMeta meta = (HCPListMeta) smi;
    HCPListData data = (HCPListData) sdi;

    Object[] row = getRow();

    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.sourcePathIndex = getInputRowMeta().indexOfValue( meta.getSourceFileField() );
      if ( data.sourcePathIndex < 0 ) {
        throw new KettleException( BaseMessages
            .getString( HCPListMeta.PKG, "HCPGet.Error.SourceFileFieldNotFound", meta.getSourceFileField() ) );
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
    }

    String sourceFilePath = getInputRowMeta().getString( row, data.sourcePathIndex );
    if ( sourceFilePath == null || sourceFilePath.length() == 0 ) {
      return true;
    }

    String restUrl = meta.getConnection().getRestUrl( this );

    if ( !sourceFilePath.substring( 0, 1 ).equals( "/" ) ) {
      sourceFilePath = '/' + sourceFilePath;
    }

    if ( !sourceFilePath.endsWith( "/" ) ) {
      sourceFilePath = sourceFilePath + "/";
    }

    long startTime = System.currentTimeMillis();

    String requestUrl = restUrl + sourceFilePath;
    if ( log.isDebug() ) {
      log.logDebug( "Request URL : " + requestUrl );
    }

    HCPListResponse hcpResponse = null;
    try {
      hcpResponse = HCPConnectionOperationUtils.performList( data.client, requestUrl, data.authorization, log );
    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( HCPListMeta.PKG, "HCPList.Error.ErrorUsingHCPService" ), e );
    }

    long endTime = System.currentTimeMillis();
    Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
    int outputIndex = data.outputRowMeta.indexOfValue( HCPListMeta.RESPONSE_TIME_FIELD_NAME );
    if ( hcpResponse != null ) {
      outputRow[outputIndex] = endTime - startTime;

      List<Object[]> targetListEntries = hcpResponse.populateKettleRows( data.outputRowMeta, outputRow );
      if ( !hcpResponse.getSuccess() ) {
        if ( log.isDebug() ) {
          log.logDebug( hcpResponse.getExplanation() );
        }
        if ( getStepMeta().isDoingErrorHandling() ) {
          putError( data.outputRowMeta, outputRow, 1L, hcpResponse.getExplanation(), "",
              hcpResponse.getStatus().toString() );
        }
        return true;
      }

      for ( Object[] r : targetListEntries ) {
        putRow( data.outputRowMeta, r );
      }
    }

    return true;
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    // HCPGetMeta meta = (HCPGetMeta) smi;
    HCPListData data = (HCPListData) sdi;

    data.client.destroy();

    super.dispose( smi, sdi );
  }
}
