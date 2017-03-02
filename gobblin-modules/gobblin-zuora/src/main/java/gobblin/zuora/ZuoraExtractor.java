package gobblin.zuora;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.RestApiConnectionException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.QueryBasedExtractor;
import gobblin.source.extractor.extract.SourceSpecificLayer;
import gobblin.source.extractor.extract.restapi.RestApiCommand;
import gobblin.source.extractor.extract.restapi.RestApiSpecificLayer;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import gobblin.source.workunit.WorkUnit;


@Slf4j
public class ZuoraExtractor extends QueryBasedExtractor<JsonArray, JsonElement> implements SourceSpecificLayer<JsonArray, JsonElement>, RestApiSpecificLayer {
  private static final Gson gson = new Gson();
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String HOUR_FORMAT = "HH";
  private final ZuoraClient _client;
  private final ZuoraClientFilesStreamer _fileStreamer;
  private List<String> _fileIds;

  public ZuoraExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
    _client = new ZuoraClientImpl(workUnitState);
    _fileStreamer = new ZuoraClientFilesStreamer(workUnitState, _client, this);
  }

  @Override
  public Iterator<JsonElement> getRecordSet(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws DataRecordException, IOException {
    if (!this.getPullStatus()) {
      log.info("pull status false");
      return null;
    }

    if (_fileIds == null) {
      List<Command> cmds = getDataMetadata(schema, entity, workUnit, predicateList);
      CommandOutput<RestApiCommand, String> postResponse = _client.executePostRequest(cmds.get(0));
      String jobId = ZuoraClientImpl.getJobId(postResponse);
      _fileIds = _client.getFileIds(jobId);
    }
    return getData(null);
  }

  @Override
  public Iterator<JsonElement> getData(CommandOutput<?, ?> postResp)
      throws DataRecordException, IOException {
    if (!_fileStreamer.isJobFinished()) {
      return _fileStreamer.streamFiles(_fileIds).iterator();
    }
    return null;
  }

  @Override
  public void extractMetadata(String schema, String entity, WorkUnit workUnit)
      throws SchemaException, IOException {
    log.info("Extract Metadata using REST Api");
    JsonArray columnArray = new JsonArray();
    JsonArray array;
    try {
      List<Command> cmds = this.getSchemaMetadata(schema, entity);
      CommandOutput<?, ?> response = null;
      if (cmds != null) {
        response = _client.executeGetRequest(cmds.get(0));
      }
      array = this.getSchema(response);
      if (array == null) {
        log.warn("Schema not found in metadata and configurations");
        columnArray = getDefaultSchema();
      } else {
        for (JsonElement columnElement : array) {
          Schema obj = gson.fromJson(columnElement, Schema.class);
          String columnName = obj.getColumnName();

          obj.setWaterMark(
              this.isWatermarkColumn(workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY), columnName));

          if (this.isWatermarkColumn(workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY), columnName)) {
            obj.setNullable(false);
          } else if (
              this.getPrimarykeyIndex(workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY), columnName)
                  == 0) {
            // set all columns as nullable except primary key and watermark columns
            obj.setNullable(true);
          }

          obj.setPrimaryKey(
              this.getPrimarykeyIndex(workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY), columnName));

          String jsonStr = gson.toJson(obj);
          JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
          columnArray.add(jsonObject);
        }
      }

      log.info("Schema:" + columnArray);
      this.setOutputSchema(columnArray);
    } catch (Exception e) {
      throw new SchemaException("Failed to get schema using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getMaxWatermark(String schema, String entity, String watermarkColumn,
      List<Predicate> snapshotPredicateList, String watermarkSourceFormat)
      throws HighWatermarkException {
    log.debug("Get high watermark using Rest Api");
    long CalculatedHighWatermark;
    try {
      List<Command> cmds = getHighWatermarkMetadata(schema, entity, watermarkColumn, snapshotPredicateList);
      CommandOutput<RestApiCommand, String> response = executeRequest(cmds);
      CalculatedHighWatermark = this.getHighWatermark(response, watermarkColumn, watermarkSourceFormat);
    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get high watermark using rest api; error - " + e.getMessage(), e);
    }

    if (CalculatedHighWatermark != -1) {
      return CalculatedHighWatermark;
    }
    return this.workUnit.getHighWaterMark();
  }

  @Override
  public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    log.debug("Get source record count using Rest Api");
    long count;
    try {
      List<Command> cmds = getCountMetadata(schema, entity, workUnit, predicateList);
      CommandOutput<RestApiCommand, String> response = executeRequest(cmds);
      count = getCount(response);
    } catch (Exception e) {
      throw new RecordCountException("Failed to get record count using rest api; error - " + e.getMessage(), e);
    }
    if (count != 0) {
      return count;
    }
    return _fileStreamer.getTotalRecords();
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity)
      throws SchemaException {
    return null;
  }

  @Override
  public JsonArray getSchema(CommandOutput<?, ?> response)
      throws SchemaException, IOException {
    JsonArray schema = null;
    if (StringUtils.isNotBlank(this.workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA))) {
      JsonArray element = gson.fromJson(this.workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA), JsonArray.class);
      schema = element.getAsJsonArray();
    }
    return schema;
  }

  @Override
  public List<Command> getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException {
    try {
      return _client.buildPostCommand(predicateList);
    } catch (Exception e) {
      throw new DataRecordException("Failed to get RightNowCloud url for data records; error - " + e.getMessage(), e);
    }
  }

  @Override
  public String getWatermarkSourceFormat(WatermarkType watermarkType) {
    String columnFormat = null;
    switch (watermarkType) {
      case TIMESTAMP:
        columnFormat = "''yyyy-MM-dd'T'HH:mm:ss''";
        break;
      case DATE:
        columnFormat = "yyyy-MM-dd";
        break;
      default:
        log.error("Watermark type " + watermarkType.toString() + " not recognized");
    }
    return columnFormat;
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting hour predicate");
    String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, HOUR_FORMAT);
    return column + " " + operator + " '" + Formattedvalue + "'";
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting date predicate");
    String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, DATE_FORMAT);
    return column + " " + operator + " '" + Formattedvalue + "'";
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting timestamp predicate");
    String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, TIMESTAMP_FORMAT);
    return column + " " + operator + " '" + Formattedvalue + "'";
  }

  @Override
  public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn,
      List<Predicate> predicateList)
      throws HighWatermarkException {
    return null;
  }

  @Override
  public long getHighWatermark(CommandOutput<?, ?> response, String watermarkColumn, String predicateColumnFormat)
      throws HighWatermarkException {
    return -1;
  }

  @Override
  public List<Command> getCountMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    return null;
  }

  @Override
  public long getCount(CommandOutput<?, ?> response)
      throws RecordCountException {
    return -1;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap =
        ImmutableMap.<String, String>builder().put("date", "date").put("datetime", "timestamp").put("time", "time")
            .put("string", "string").put("int", "int").put("long", "long").put("float", "float").put("double", "double")
            .put("decimal", "double").put("varchar", "string").put("boolean", "boolean").build();
    return dataTypeMap;
  }

  @Override
  public HttpEntity getAuthentication()
      throws RestApiConnectionException {
    return null;
  }

  @Override
  public String getNextUrl() {
    return null;
  }

  public List<String> extractHeader(ArrayList<String> firstLine) {
    List<String> header = ZuoraUtil.getHeader(firstLine);
    if (StringUtils.isBlank(workUnitState.getProp(ConfigurationKeys.SOURCE_SCHEMA))) {
      List<String> timeStampColumns = Lists.newArrayList();
      String timeStampColumnString = workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_TIMESTAMP_COLUMNS);
      if (StringUtils.isNotBlank(timeStampColumnString)) {
        timeStampColumns = Arrays.asList(timeStampColumnString.toLowerCase().replaceAll(" ", "").split(","));
      }
      setSchema(header, timeStampColumns);
    }
    log.info("record header:" + header);
    return header;
  }

  private void setSchema(List<String> cols, List<String> timestampColumns) {
    JsonArray columnArray = new JsonArray();
    for (String columnName : cols) {
      Schema obj = new Schema();
      obj.setColumnName(columnName);
      obj.setComment("resolved");
      obj.setWaterMark(isWatermarkColumn(workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY), columnName));

      if (isWatermarkColumn(workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY), columnName)) {
        obj.setNullable(false);
        obj.setDataType(convertDataType(columnName, "timestamp", null, null));
      } else if (getPrimarykeyIndex(workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY), columnName)
          == 0) {
        // set all columns as nullable except primary key and watermark columns
        obj.setNullable(true);
      }

      if (timestampColumns != null && timestampColumns.contains(columnName.toLowerCase())) {
        obj.setDataType(convertDataType(columnName, "timestamp", null, null));
      }

      obj.setPrimaryKey(
          getPrimarykeyIndex(workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY), columnName));

      String jsonStr = gson.toJson(obj);
      JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
      columnArray.add(jsonObject);
    }

    log.info("Resolved Schema:" + columnArray);
    this.setOutputSchema(columnArray);
  }

  private CommandOutput<RestApiCommand, String> executeRequest(List<Command> cmds)
      throws Exception {
    if (cmds == null || cmds.isEmpty()) {
      return null;
    }
    Command cmd = cmds.get(0);
    RestApiCommand.RestApiCommandType commandType = (RestApiCommand.RestApiCommandType) cmd.getCommandType();
    CommandOutput<RestApiCommand, String> output = null;
    switch (commandType) {
      case GET:
        output = _client.executeGetRequest(cmd);
        break;
      case POST:
        output = _client.executePostRequest(cmd);
        break;
      default:
        log.error("Invalid REST API command type " + commandType);
        break;
    }
    return output;
  }

  private JsonArray getDefaultSchema() {
    JsonArray columnArray = new JsonArray();
    String pk = workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
    if (StringUtils.isNotBlank(pk)) {
      List<String> pkCols = Arrays.asList(pk.replaceAll(" ", "").split(","));
      for (String col : pkCols) {
        Schema obj = new Schema();
        obj.setColumnName(col);
        obj.setDataType(convertDataType(col, null, null, null));
        obj.setComment("default");
        String jsonStr = gson.toJson(obj);
        JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        columnArray.add(jsonObject);
      }
    }

    String watermark = workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
    if (StringUtils.isNotBlank(watermark)) {
      List<String> watermarkCols = Arrays.asList(watermark.replaceAll(" ", "").split(","));
      for (String col : watermarkCols) {
        Schema obj = new Schema();
        obj.setColumnName(col);
        obj.setDataType(convertDataType(col, null, null, null));
        obj.setComment("default");
        obj.setWaterMark(true);
        String jsonStr = gson.toJson(obj);
        JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        columnArray.add(jsonObject);
      }
    }
    return columnArray;
  }

  @Override
  public void closeConnection()
      throws Exception {
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws IOException {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void setTimeOut(int timeOut) {

  }

  public boolean getPullStatus() {
    return true;
  }
}
