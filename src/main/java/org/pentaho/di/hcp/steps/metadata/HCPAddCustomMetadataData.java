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

package org.pentaho.di.hcp.steps.metadata;

import com.sun.jersey.client.apache.ApacheHttpClient;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class HCPAddCustomMetadataData extends BaseStepData implements StepDataInterface {

  public ApacheHttpClient m_client;
  public int m_sourcePathIndex;
  public int m_metadataAnnotationIndex;
  public int m_targetPathIndex;
  public String m_targetPrependPath;
  public String m_authorization;
  public RowMetaInterface m_outputRowMeta;

  public int m_sysMetaIndexIndex;
  public int m_sysMetaShredIndex;
  public int m_sysMetaHoldIndex;
  public int m_sysMetaRetentionIndex;

}
