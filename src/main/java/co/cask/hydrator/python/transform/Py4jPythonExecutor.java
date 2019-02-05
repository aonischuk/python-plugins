/*
 * Copyright © 2019
  * Cask Data, Inc.
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

package co.cask.hydrator.python.transform;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.hydrator.common.script.ScriptContext;
import org.apache.commons.io.FileUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;
import py4j.Py4JException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Executes python code using py4j python library.
 * Which involves starting python process as a server and than connecting java client to it.
 */
public class Py4jPythonExecutor implements  PythonExecutor {
  private static final int PY4J_INIT_POLL_INTERVAL = 1;
  private static final int PY4J_INIT_TIMEOUT = 120;
  private static final int PYTHON_PROCESS_PORT_FILE_CREATED_INTERVAL = 1;
  private static final int PYTHON_PROCESS_PORT_FILE_CREATED_TIMEOUT = 120;
  private static final int PYTHON_PROCESS_STOP_TIMEOUT = 30;
  private static final int PYTHON_PROCESS_KILL_TIMEOUT = 10;
  private static final String USER_CODE_PLACEHOLDER = "\\$\\{cdap\\.transform\\.function\\}";

  private final PythonEvaluator.Config config;
  private File pythonProcessOutputFile;
  private Logger logger;
  private Process process;
  private GatewayServer gatewayServer;
  private Py4jTransport py4jTransport;
  private File transformTempDir;

  public Py4jPythonExecutor(PythonEvaluator.Config config) {
    this.config = config;
  }

  private File prepareTempFiles() throws IOException {
    URL url = getClass().getResource("/pythonEvaluator.py");
    String scriptText = new String(Files.readAllBytes(Paths.get(url.getPath())), StandardCharsets.UTF_8);
    scriptText = scriptText.replaceAll(USER_CODE_PLACEHOLDER, config.getScript());

    Path transformTempDirPath = Files.createTempDirectory("transform");

    logger.debug("Tmp folder is {}", transformTempDirPath);

    String transformTempDirString = transformTempDirPath.normalize().toString();
    transformTempDir = new File(transformTempDirString);

    pythonProcessOutputFile = new File(transformTempDirString, "output.txt");
    pythonProcessOutputFile.createNewFile();

    File tempPythonFile = new File(transformTempDirString, "transform.py");
    tempPythonFile.createNewFile();

    PrintWriter out = new PrintWriter(tempPythonFile);
    out.write(scriptText);
    out.close();

    return tempPythonFile;
  }

  @Override
  public void initialize(ScriptContext scriptContext) throws IOException, InterruptedException {
    logger = LoggerFactory.getLogger(Py4jPythonExecutor.class.getName());

    File tempPythonFile = prepareTempFiles();
    File portFile = new File(transformTempDir, "port");

    String pythonBinary = "python3"; // TODO: customize when widget is ready
    ProcessBuilder   ps = new ProcessBuilder(pythonBinary, tempPythonFile.getPath());
    ps.directory(transformTempDir);
    ps.redirectErrorStream(true);
    ps.redirectOutput(pythonProcessOutputFile);
    process = ps.start();

    logger.debug("Waiting for python process to respond with port...");

    try {
      Awaitility.with()
              .pollInterval(PYTHON_PROCESS_PORT_FILE_CREATED_INTERVAL, SECONDS)
              .atMost(PYTHON_PROCESS_PORT_FILE_CREATED_TIMEOUT, SECONDS)
              .await()
              .until(() -> (portFileCreated(portFile)));
    } catch (ConditionTimeoutException e) {
      String message =  String.format("Port file was not created by python process in %d seconds.\n%s",
              PYTHON_PROCESS_PORT_FILE_CREATED_TIMEOUT, getProcessOutput());
      throw new RuntimeException(message);
    }

    Integer pythonPort = Files.lines(portFile.toPath()).map(s -> Integer.parseInt(s)).findFirst().get();

    logger.debug("Python process port is: {}", pythonPort);

    gatewayServer = new GatewayServer((Object) null, 0, pythonPort, 0, 0, (List) null);
    gatewayServer.start();

    Class[] entryClasses = new Class[]{Py4jTransport.class};
    py4jTransport = (Py4jTransport) gatewayServer.getPythonServerEntryPoint(entryClasses);

    logger.debug("Waiting for py4j gateway to start...");

    try {
      Awaitility.with()
              .pollInterval(PY4J_INIT_POLL_INTERVAL, SECONDS)
              .atMost(PY4J_INIT_TIMEOUT, SECONDS)
              .await()
              .until(() -> (initializedPythonProcess()));
    } catch (ConditionTimeoutException e) {
      String message =  String.format("Connection to python process failed in %d seconds.\n%s",
              PY4J_INIT_TIMEOUT, getProcessOutput());
      throw new RuntimeException(message);
    }
  }

  @Override
  public void transform(StructuredRecord input, final Emitter<StructuredRecord> emitter,
                        Emitter<Map> pythonEmitter, ScriptContext scriptContext) {
    try {
      Object record = PythonObjectsEncoder.encode(input, input.getSchema());
      py4jTransport.transform(record, pythonEmitter, scriptContext);
    } catch (Exception e) {
      if (e instanceof Py4JException) {
        logger.error(e.getMessage());
      }
      throw new IllegalArgumentException("Could not transform input.\n" + e.getMessage());
    }
  }

  @Override
  public void destroy() {
    try {
      if (py4jTransport != null) {
        py4jTransport.finish();
      }
    } catch (Exception e) {
      logger.warn("Cannot close python process.\n", e);
    }

    if (gatewayServer != null) {
      gatewayServer.shutdown();
    }

    logger.debug("Waiting for python process to finish");

    if (process == null) {
      logger.debug("Process was not created. Not waiting for it to finish.");
      return;
    }

    try {
      process.waitFor(PYTHON_PROCESS_STOP_TIMEOUT, SECONDS);
      process.destroy(); // tell the process to stop
      process.waitFor(PYTHON_PROCESS_KILL_TIMEOUT, SECONDS); // give it a chance to stop
      process.destroyForcibly();
    } catch (InterruptedException e) {
      int overallTimeout = PYTHON_PROCESS_STOP_TIMEOUT + PYTHON_PROCESS_KILL_TIMEOUT;
      logger.warn("Python process was not able to stop in {} seconds.", overallTimeout);
    }

    logger.info("Python process output:\n{}", getProcessOutput());

    try {
      FileUtils.deleteDirectory(transformTempDir);
    } catch (IOException e) {
      logger.warn("Cannot delete tmp directory {}", transformTempDir, e);
    }
  }

  private String getProcessOutput() {
    Path pythonProcessOutputFilePath = Paths.get(pythonProcessOutputFile.getPath());

    try {
      String processOutput = new String(Files.readAllBytes(pythonProcessOutputFilePath), StandardCharsets.UTF_8);
      return processOutput;
    } catch (IOException e) {
      logger.warn("Cannot read output of python process.\n", e);
    }
    return null;
  }

  private boolean portFileCreated(File portFile) {
    // usually happens if code has a syntax error
    if (!process.isAlive()) {
      throw new RuntimeException(String.format("Python process died too early.\n%s", getProcessOutput()));
    }

    return portFile.exists();
  }

  private boolean initializedPythonProcess() {
    try {
      py4jTransport.initialize(gatewayServer.getListeningPort());
      return true;
    } catch (py4j.Py4JException e) {
    }
    return false;
  }
}
