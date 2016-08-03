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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Move file(s) within HDFS.
 * A user must specify file/directory path and destination file/directory path
 * Optionals include fileRegex
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("HDFSAction")
@Description("Action to move files within HDFS")
public class HDFSAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSAction.class);

  private HDFSActionConfig config;

  public HDFSAction(HDFSActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FileSystem hdfs = new Path(config.sourcePath).getFileSystem(new Configuration());

    if (new File(config.sourcePath).isFile()) {//moving single file
      Path source = new Path(config.sourcePath);
      Path dest = new Path(config.destPath);
      try {
        hdfs.rename(source, dest);
      } catch (IOException e) {
        LOG.error("Failed to move file {} to {} for reason: {}", source.toString(), dest.toString(), e);
      }
      return;
    }

    //moving contents of directory
    File folder = new File(config.sourcePath);
    File[] listFiles;
    if (config.fileRegex != null) {
      FileFilter filter = new WildcardFileFilter(config.fileRegex);
      listFiles = folder.listFiles(filter);
    } else {
      listFiles = folder.listFiles();
    }

    for (int i = 0; i < listFiles.length; i++) {
      Path source = new Path(listFiles[i].getAbsolutePath());
      Path dest;
      //Moving directory, so destination filePath doesn't have filename
      dest = new Path(config.destPath + listFiles[i].getName());

      try {
        hdfs.rename(source, dest);
      } catch (IOException e) {
        LOG.error("Failed to move file {} to {} for reason: {}", source.toString(), dest.toString(), e);
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

  }

  /**
   * Config class that contains all properties necessary to execute an HDFS move command.
   */
  public class HDFSActionConfig extends PluginConfig {
    @Description("Full HDFS path of file/directory")
    private String sourcePath;

    @Description("Full HDFS path of desired destination path")
    private String destPath;

    @Nullable
    private String fileRegex;

    HDFSActionConfig(String sourcePath, String destPath, String fileRegex) {
      this.sourcePath = sourcePath;
      this.destPath = destPath;
      this.fileRegex = fileRegex;
    }
  }
}
