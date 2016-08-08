/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.db.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.hydrator.plugin.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;

/**
 * Action that runs a database command (e.g. query, update)
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("DBAction")
@Description("Action that runs a database command (e.g. query, update)")
public class DBAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(DBAction.class);
  private final QueryConfig config;

  public DBAction(QueryConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {

    Class<? extends Driver> driverClass = context.loadPluginClass(config.JDBC_PLUGIN_ID);
      config.executeQuery(driverClass, LOG, config);


  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    DBManager dbManager = new DBManager(config);
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, config.JDBC_PLUGIN_ID);
  }
}
