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

import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link HDFSAction}
 */
public class HDFSActionTestRun extends ETLBatchTestBase {
  private final String sourcePath = "/some/Source/Path";
  private final String destPath = "/some/dest/Path";

  @Test
  public void testHDFSAction() throws Exception {
    File file = new File("testFiles/source/test.txt");
    file.getParentFile().mkdirs();
    file.createNewFile();
    File jsonFile = new File("testFiles/source/test.json");
    jsonFile.createNewFile();

    ETLStage action = new ETLStage(
      "HDFSAction",
      new ETLPlugin("HDFSAction", Action.PLUGIN_TYPE,
                    ImmutableMap.of("sourcePath", "testFiles/source",
                                    "destPath", "testFiles/",
                                    "fileRegex", ".*\\.txt"),
                    null));
    ETLStage source = new ETLStage("source",
                                   new ETLPlugin("KVTable", BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of("name", "hdfsTestSource"), null));
    ETLStage sink = new ETLStage("sink", new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                                                       ImmutableMap.of("name", "hdfsTestSink"), null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(action)
      .addConnection(action.getName(), source.getName())
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sshActionTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.start(ImmutableMap.of("logical.start.time", "0"));
    manager.waitForFinish(3, TimeUnit.MINUTES);

    File textDestFile = new File("testFiles/test.txt");
    Assert.assertTrue(textDestFile.exists());
    File jsonDestFile = new File("testFiles/test.json");
    Assert.assertFalse(jsonDestFile.exists());


    File dir = new File("testFiles");
    deleteDirectory(dir);

  }

  public static boolean deleteDirectory(File directory) {
    if(directory.exists()){
      File[] files = directory.listFiles();
      if(null!=files){
        for(int i=0; i<files.length; i++) {
          if(files[i].isDirectory()) {
            deleteDirectory(files[i]);
          }
          else {
            files[i].delete();
          }
        }
      }
    }
    return(directory.delete());
  }
}
