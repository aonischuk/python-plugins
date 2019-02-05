/*
 * Copyright © 2019 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldTransformOperation;
import co.cask.hydrator.common.SchemaValidator;
import co.cask.hydrator.common.script.JavaTypeConverters;
import co.cask.hydrator.common.script.ScriptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Transforms records using custom Python code provided by the config.
 */
@Plugin(type = "transform")
@Name("PythonEvaluator")
@Description("Executes user-provided Python code that transforms one record into another.")
public class PythonEvaluator extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private Schema schema;
  private StageMetrics metrics;
  private Logger logger;
  private ScriptContext scriptContext;
  private PythonExecutor pythonExecutor;

  /**
   * Configuration for the script transform.
   */
  public static class Config extends PluginConfig {
    @Description("Python code defining how to transform one record into another. The script must implement a " +
      "function called 'transform', which takes as input a dictionary representing the input record, an emitter " +
      "object, and a context object (which contains CDAP metrics and logger). The emitter object can be used to emit " +
      "one or more key-value pairs to the next stage. It can also be used to emit errors. " +
      "For example:\n" +
      "'def transform(record, emitter, context):\n" +
      "  if record['count'] == 0:\n" +
      "    emitter.emitError({\"errorCode\":31, \"errorMsg\":\"Count is zero.\", \"invalidRecord\": record}\n" +
      "    return\n" +
      "  record['count'] *= 1024\n" +
      "  if(record['count'] < 0):\n" +
      "    context.getMetrics().count(\"negative.count\", 1)\n" +
      "    context.getLogger().debug(\"Received record with negative count\")\n" +
      "  emitter.emit(record)'\n" +
      "will scale the 'count' field by 1024.")
    private final String script;

    @Description("The schema of the output object. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    private final String schema;

    public Config(String script, String schema) {
      this.script = script;
      this.schema = schema;
    }

    public String getScript() {
      return script;
    }
  }

  // for unit tests, otherwise config is injected by plugin framework.
  public PythonEvaluator(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws IOException, InterruptedException {
    metrics = context.getMetrics();
    logger = LoggerFactory.getLogger(PythonEvaluator.class.getName() + " - Stage:" + context.getStageName());

    init(context);

    pythonExecutor = new Py4jPythonExecutor(config); // TODO: customize when widget is ready
    pythonExecutor.initialize(scriptContext);
  }

  @Override
  public void destroy() {
    pythonExecutor.destroy();
  }


  /**
   * Emitter to be used from within Python code
   */
  public final class PythonEmitter implements Emitter<Map> {

    private final Emitter<StructuredRecord> emitter;
    private final Schema schema;

    public PythonEmitter(Emitter<StructuredRecord> emitter, Schema schema) {
      this.emitter = emitter;
      this.schema = schema;
    }

    @Override
    public void emit(Map value) {
      emitter.emit(decode(value));
    }

    @Override
    public void emitAlert(Map<String, String> payload) {
      emitter.emitAlert(payload);
    }

    @Override
    public void emitError(InvalidEntry<Map> invalidEntry) {
      emitter.emitError(new InvalidEntry<>(invalidEntry.getErrorCode(), invalidEntry.getErrorMsg(),
                                           decode(invalidEntry.getInvalidRecord())));
    }

    public void emitError(Map invalidEntry) {
      emitter.emitError(new InvalidEntry<>((int) invalidEntry.get("errorCode"),
                                           (String) invalidEntry.get("errorMsg"),
                                           decode((Map) invalidEntry.get("invalidRecord"))));
    }

    private StructuredRecord decode(Map nativeObject) {
      return PythonObjectsEncoder.decodeRecord(nativeObject, schema);
    }
  }


  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    if (config.schema != null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(parseJson(config.schema));
    } else {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
    }
    // try evaluating the script to fail application creation if the script is invalid
    init(null);
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    List<String> inputFields = new ArrayList<>();
    List<String> outputFields = new ArrayList<>();
    Schema inputSchema = context.getInputSchema();
    if (SchemaValidator.canRecordLineage(inputSchema, "input")) {
      //noinspection ConstantConditions
      inputFields = inputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    }
    Schema outputSchema = context.getOutputSchema();
    if (SchemaValidator.canRecordLineage(outputSchema, "output")) {
      //noinspection ConstantConditions
      outputFields = outputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    }
    FieldOperation dataPrepOperation = new FieldTransformOperation("Python", config.script, inputFields, outputFields);
    context.record(Collections.singletonList(dataPrepOperation));
  }

  @Override
  public void transform(StructuredRecord input, final Emitter<StructuredRecord> emitter) {
    Emitter<Map> pythonEmitter = new PythonEvaluator.PythonEmitter(emitter,
            schema == null ? input.getSchema() : schema);
    pythonExecutor.transform(input, emitter, pythonEmitter, scriptContext);
  }

  private void init(@Nullable TransformContext context) {
    scriptContext = new ScriptContext(
      logger, metrics,
      new LookupProvider() {
        @Override
        public <T> Lookup<T> provide(String s, Map<String, String> map) {
          throw new UnsupportedOperationException("lookup is currently not supported.");
        }
      },
      null,
      new JavaTypeConverters() {
        @Override
        public Object mapToJSObject(Map<?, ?> map) {
          return null;
        }
      },
      context == null ? null : context.getArguments());

    if (config.schema != null) {
      schema = parseJson(config.schema);
    }
  }

  private Schema parseJson(String schema) {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
    }
  }

}

