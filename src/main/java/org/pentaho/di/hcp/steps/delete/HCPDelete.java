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

package org.pentaho.di.hcp.steps.delete;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.hcp.shared.BaseHCPResponse;
import org.pentaho.di.hcp.shared.HCPConnectionOperationUtils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class HCPDelete extends BaseStep implements StepInterface {
  private static Class<?> PKG = HCPDelete.class; // for i18n purposes, needed by Translator2!!

  public HCPDelete( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    HCPDeleteMeta meta = (HCPDeleteMeta) smi;
    HCPDeleteData data = (HCPDeleteData) sdi;

    boolean error = false;
    if ( meta.getConnection() == null ) {
      log.logError( BaseMessages.getString( PKG, "HCPDelete.Error.HCPConnectionNotSpecified" ) );
      error = true;
    }
    if ( StringUtils.isEmpty( meta.getTargetFileField() ) ) {
      log.logError( BaseMessages.getString( PKG, "HCPDelete.Error.TargetFileFieldNotSpecified" ) );
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

    HCPDeleteMeta meta = (HCPDeleteMeta) smi;
    HCPDeleteData data = (HCPDeleteData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.targetPathIndex = getInputRowMeta().indexOfValue( meta.getTargetFileField() );
      if ( data.targetPathIndex < 0 ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "HCPDelete.Error.TargetFileFieldNotFound", meta.getTargetFileField() ) );
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
    }

    String targetFilePath = getInputRowMeta().getString( row, data.targetPathIndex );

    if ( StringUtils.isEmpty( targetFilePath ) ) {
      log.logError( "An empty target file path is not supported at this time. " );
      stopAll();
      return false;
    }

    long startTime = System.currentTimeMillis();
    int responseCode = -1;

    String restUrl = meta.getConnection().getRestUrl( this );

    // Remove last slash character of URL
    //
    if ( restUrl.length() > 0 && restUrl.charAt( restUrl.length() - 1 ) == '/' ) {
      restUrl = restUrl.substring( 0, restUrl.length() - 1 );
    }

    // Add slash to start of target path
    //
    if ( !targetFilePath.substring( 0, 1 ).equals( "/" ) ) {
      targetFilePath = '/' + targetFilePath;
    }

    // Calculate the URL for the target file...
    //
    String requestUrl = restUrl + targetFilePath;
    if ( log.isDebug() ) {
      log.logDebug( "Request URL : " + requestUrl );
    }

    WebResource webResource = data.client.resource( requestUrl );
    Builder builder = webResource.getRequestBuilder().header( "Authorization", data.authorization );
    BaseHCPResponse hcpResponse = null;
    try {

      // Execute an HTTP PUT. This tells HCP to store the data being provided
      ClientResponse response;
      hcpResponse = HCPConnectionOperationUtils.performDelete( data.client, requestUrl, data.authorization, log );

      // ClientResponse response = builder.type( MediaType.APPLICATION_OCTET_STREAM_TYPE ).delete( ClientResponse.class );

      /* responseCode = response.getStatus();
      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "HCPDelete.StatusCode", requestUrl, responseCode ) );
      }
     */
    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "HCPDelete.Error.ErrorUsingHCPService" ), e );
    }

    long endTime = System.currentTimeMillis();

    if ( hcpResponse != null ) {
      Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
      int outputIndex = getInputRowMeta().size();
      outputRow[outputIndex++] = endTime - startTime;
      hcpResponse.populateKettleRow( data.outputRowMeta, outputRow );

      if ( !hcpResponse.getSuccess() ) {
        if ( log.isDebug() ) {
          log.logDebug( hcpResponse.getExplanation() );
        }
        // we have an error of some sort - output an error row?
        if ( getStepMeta().isDoingErrorHandling() ) {
          putError( data.outputRowMeta, outputRow, 1L, hcpResponse.getExplanation(), "",
              hcpResponse.getStatus().toString() );

          return true;
        }
      }

      putRow( data.outputRowMeta, outputRow );
    }

    return true;
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    // HCPDeleteMeta meta = (HCPDeleteMeta) smi;
    HCPDeleteData data = (HCPDeleteData) sdi;

    data.client.destroy();

    super.dispose( smi, sdi );
  }
}
