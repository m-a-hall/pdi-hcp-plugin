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

package org.pentaho.di.hcp.steps.get;

import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.hcp.shared.HCPConnectionOperationUtils;
import org.pentaho.di.hcp.shared.HCPReadResponse;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

public class HCPGet extends BaseStep implements StepInterface {
  private static Class<?> PKG = HCPGet.class; // for i18n purposes, needed by
  // Translator2!!

  public HCPGet( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    HCPGetMeta meta = (HCPGetMeta) smi;
    HCPGetData data = (HCPGetData) sdi;

    boolean error = false;
    if ( meta.getConnection() == null ) {
      log.logError( BaseMessages.getString( PKG, "HCPGet.Error.HCPConnectionNotSpecified" ) );
      error = true;
    }
    if ( StringUtils.isEmpty( meta.getSourceFileField() ) ) {
      log.logError( BaseMessages.getString( PKG, "HCPGet.Error.SourceFileFieldNotSpecified" ) );
      error = true;
    }
    if ( StringUtils.isEmpty( meta.getTargetFileField() ) && !meta.getFetchSystemMetadataOnly() ) {
      log.logError( BaseMessages.getString( PKG, "HCPGet.Error.TargetFileFieldNotSpecified" ) );
      error = true;
    }
    if ( error ) {
      // Stop right here.
      return false;
    }

    data.bufferSize = 1024;
    data.authorization = meta.getConnection().getAuthorizationHeader();

    data.client = ApacheHttpClient.create( new DefaultApacheHttpClientConfig() );
    data.client.setChunkedEncodingSize( data.bufferSize );

    return super.init( smi, sdi );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    HCPGetMeta meta = (HCPGetMeta) smi;
    HCPGetData data = (HCPGetData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.sourcePathIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getSourceFileField() ) );
      if ( data.sourcePathIndex < 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, "HCPGet.Error.SourceFileFieldNotFound",
            environmentSubstitute( meta.getSourceFileField() ) ) );
      }
      data.targetPathIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getTargetFileField() ) );
      if ( data.targetPathIndex < 0 ) {
        throw new KettleException( BaseMessages.getString( PKG, "HCPGet.Error.TargetFileFieldNotFound",
            environmentSubstitute( meta.getTargetFileField() ) ) );
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
    }

    String sourceFilePath = getInputRowMeta().getString( row, data.sourcePathIndex );
    if ( sourceFilePath == null || sourceFilePath.length() == 0 ) {
      return true;
    }

    String targetFilePath = getInputRowMeta().getString( row, data.targetPathIndex );

    String restUrl = meta.getConnection().getRestUrl( this );

    // Remove last slash character of URL
    //
    if ( restUrl.length() > 0 && restUrl.charAt( restUrl.length() - 1 ) == '/' ) {
      restUrl = restUrl.substring( 0, restUrl.length() - 1 );
    }

    // Add slash to start of source (HCP) path
    //
    if ( !sourceFilePath.substring( 0, 1 ).equals( "/" ) ) {
      sourceFilePath = '/' + sourceFilePath;
    }

    long startTime = System.currentTimeMillis();

    // Calculate the URL for the target file...
    //
    String requestUrl = restUrl + sourceFilePath;
    if ( log.isDebug() ) {
      log.logDebug( "Request URL : " + requestUrl );
    }

    OutputStream outputStream = null;

    HCPReadResponse hcpResponse = null;
    try {
      outputStream = !meta.getFetchSystemMetadataOnly() ? KettleVFS.getOutputStream( targetFilePath, false ) : null;
      hcpResponse =
          HCPConnectionOperationUtils.performGet( data.client, requestUrl, data.authorization,
              !meta.getFetchSystemMetadataOnly() ? new BufferedOutputStream( outputStream ) : null, log );

    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "HCPGet.Error.ErrorUsingHCPService" ), e );
    }

    long endTime = System.currentTimeMillis();

    Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
    int outputIndex = data.outputRowMeta.indexOfValue( HCPGetMeta.RESPONSE_TIME_FIELD_NAME );

    if ( hcpResponse != null ) {
      outputRow[outputIndex] = endTime - startTime;
      hcpResponse.populateKettleRow( data.outputRowMeta, outputRow );

      if ( !hcpResponse.getSuccess() ) {
        if ( log.isDebug() ) {
          log.logDebug( hcpResponse.getExplanation() );
        }
        if ( getStepMeta().isDoingErrorHandling() ) {
          putError( data.outputRowMeta, outputRow, 1L, hcpResponse.getExplanation(), "",
              hcpResponse.getStatus().toString() );

          return true;
        }
      }
    }

    putRow( data.outputRowMeta, outputRow );

    return true;
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    // HCPGetMeta meta = (HCPGetMeta) smi;
    HCPGetData data = (HCPGetData) sdi;

    data.client.destroy();

    super.dispose( smi, sdi );
  }
}
