package gobblin.zuora;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

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
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.QueryBasedExtractor;
import gobblin.source.extractor.extract.restapi.RestApiCommand;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import gobblin.source.workunit.WorkUnit;


@Slf4j
public class ZuoraExtractor extends QueryBasedExtractor<JsonArray, JsonElement> {
  private static final Gson GSON = new Gson();
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String HOUR_FORMAT = "HH";
  private final ZuoraClient _client;
  private final ZuoraClientFilesStreamer _fileStreamer;
  private List<String> _fileIds;
  private List<String> _header;

  public ZuoraExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
    _client = new ZuoraClientImpl(workUnitState);
    _fileStreamer = new ZuoraClientFilesStreamer(workUnitState, _client);
  }

  @Override
  public Iterator<JsonElement> getRecordSet(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws DataRecordException, IOException {
    if (_fileIds == null) {
      List<Command> cmds = _client.buildPostCommand(predicateList);
      CommandOutput<RestApiCommand, String> postResponse = _client.executePostRequest(cmds.get(0));
      String jobId = ZuoraClientImpl.getJobId(postResponse);
      _fileIds = _client.getFileIds(jobId);
    }

    if (!_fileStreamer.isJobFinished()) {
      return _fileStreamer.streamFiles(_fileIds, _header).iterator();
    }
    return null;
  }

  @Override
  public void extractMetadata(String schema, String entity, WorkUnit workUnit)
      throws SchemaException, IOException {
    String deltaFields = workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
    String primaryKeyColumn = workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
    JsonArray columnArray = new JsonArray();
    _header = new ArrayList<>();

    try {
      JsonArray array =
          GSON.fromJson(workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA), JsonArray.class).getAsJsonArray();
      for (JsonElement columnElement : array) {
        Schema obj = GSON.fromJson(columnElement, Schema.class);
        String columnName = obj.getColumnName();
        _header.add(columnName);

        boolean isWaterMarkColumn = isWatermarkColumn(deltaFields, columnName);
        if (isWaterMarkColumn) {
          obj.setWaterMark(true);
          obj.setNullable(false);
        }

        int primarykeyIndex = getPrimarykeyIndex(primaryKeyColumn, columnName);
        obj.setPrimaryKey(primarykeyIndex);
        boolean isPrimaryKeyColumn = primarykeyIndex > 0;
        if (isPrimaryKeyColumn) {
          obj.setNullable(false);
        }

        String jsonStr = GSON.toJson(obj);
        JsonObject jsonObject = GSON.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        columnArray.add(jsonObject);
      }

      log.info("Update Schema is:" + columnArray);
      setOutputSchema(columnArray);
    } catch (Exception e) {
      throw new SchemaException("Failed to get schema using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getMaxWatermark(String schema, String entity, String watermarkColumn,
      List<Predicate> snapshotPredicateList, String watermarkSourceFormat)
      throws HighWatermarkException {
    throw new HighWatermarkException(
        "GetMaxWatermark with query is not supported! Please set source.querybased.skip.high.watermark.calc to true.");
  }

  @Override
  public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    //Set source.querybased.skip.count.calc to true will set SourceCount to -1. However, ...

    //This ExpectedRecordCount will determine tablesWithNoUpdatesOnPreviousRun in QueryBasedSource.
    //We need to return a positive number to bypass this check and move Low watermark forward.
    return 1;
  }

  @Override
  public String getWatermarkSourceFormat(WatermarkType watermarkType) {
    switch (watermarkType) {
      case TIMESTAMP:
        return TIMESTAMP_FORMAT;
      case DATE:
        return DATE_FORMAT;
      case HOUR:
        return HOUR_FORMAT;
      default:
        throw new RuntimeException("Watermark type " + watermarkType.toString() + " not supported");
    }
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    String hourPredicate = String
        .format("%s %s '%s'", column, operator, Utils.toDateTimeFormat(Long.toString(value), valueFormat, HOUR_FORMAT));
    log.info("Hour predicate is: " + hourPredicate);
    return hourPredicate;
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    String datePredicate = String
        .format("%s %s '%s'", column, operator, Utils.toDateTimeFormat(Long.toString(value), valueFormat, DATE_FORMAT));
    log.info("Date predicate is: " + datePredicate);
    return datePredicate;
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    String timeStampPredicate = String.format("%s %s '%s'", column, operator,
        Utils.toDateTimeFormat(Long.toString(value), valueFormat, TIMESTAMP_FORMAT));
    log.info("Timestamp predicate is: " + timeStampPredicate);
    return timeStampPredicate;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap =
        ImmutableMap.<String, String>builder().put("date", "date").put("datetime", "timestamp").put("time", "time")
            .put("string", "string").put("int", "int").put("long", "long").put("float", "float").put("double", "double")
            .put("decimal", "double").put("varchar", "string").put("boolean", "boolean").build();
    return dataTypeMap;
  }

  List<String> extractHeader(ArrayList<String> firstLine) {
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

      String jsonStr = GSON.toJson(obj);
      JsonObject jsonObject = GSON.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
      columnArray.add(jsonObject);
    }

    log.info("Resolved Schema:" + columnArray);
    this.setOutputSchema(columnArray);
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
}
