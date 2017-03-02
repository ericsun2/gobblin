package gobblin.zuora;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import gobblin.source.extractor.extract.jdbc.SqlQueryUtils;
import gobblin.source.extractor.extract.restapi.BasicRestApiExtractor;
import gobblin.source.extractor.extract.restapi.RestApiCommand;
import gobblin.source.extractor.extract.restapi.RestApiCommand.RestApiCommandType;
import gobblin.source.extractor.resultset.RecordSet;
import gobblin.source.extractor.resultset.RecordSetList;
import gobblin.source.extractor.utils.InputStreamCSVReader;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import gobblin.source.workunit.WorkUnit;


@Slf4j
public class ZuoraExtractor extends BasicRestApiExtractor {
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String HOUR_FORMAT = "HH";
  private final int _batchSize;
  private BufferedReader _currentReader;
  private int _currentFileIndex = 0;
  private List<String> _header = null;
  private boolean _jobFinished = false;

  public ZuoraExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
    _batchSize = workUnit
        .getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE, ConfigurationKeys.DEFAULT_SOURCE_FETCH_SIZE);
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
      String host = getEndPoint("batch-query/");
      List<String> params = Lists.newLinkedList();
      params.add(host);

      String query = workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY,
          "SELECT * FROM " + workUnitState.getProp(ConfigurationKeys.SOURCE_ENTITY));

      if (predicateList != null) {
        for (Predicate predicate : predicateList) {
          query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
        }
      }

      String rowLimit = workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_ROW_LIMIT);
      if (StringUtils.isNotBlank(rowLimit)) {
        query += " LIMIT " + rowLimit;
      }

      List<ZuoraQuery> queries = Lists.newArrayList();
      queries.add(new ZuoraQuery(workUnitState.getProp(ConfigurationKeys.JOB_NAME_KEY), query));
      ZuoraParams filterPayload = new ZuoraParams(workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_PARTNER, "sample"),
          workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_PROJECT, "sample"), queries,
          workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_API_NAME, "sample"),
          workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_OUTPUT_FORMAT, "csv"),
          workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_VERSION, "1.1"));
      params.add(gson.toJson(filterPayload));
      return Collections.singletonList(new RestApiCommand().build(params, RestApiCommandType.POST));
    } catch (Exception e) {
      throw new DataRecordException("Failed to get RightNowCloud url for data records; error - " + e.getMessage(), e);
    }
  }

  @Override
  public Iterator<JsonElement> getData(CommandOutput<?, ?> response)
      throws DataRecordException, IOException {
    try {
      List<String> _fileList = null;
      if (response != null) {
        Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
        if (!itr.hasNext()) {
          throw new DataRecordException("Failed to get data from RightNowCloud; REST response has no output");
        }

        String stringResponse = itr.next();
        log.info("Batch query result: " + stringResponse);
        JsonObject jsonObject = gson.fromJson(stringResponse, JsonObject.class).getAsJsonObject();
        String jobId = jsonObject.get("id").getAsString();
        _fileList = getFiles(jobId);
      }

      RecordSet<JsonElement> rs = null;
      if (!_jobFinished) {
        rs = streamFiles(_fileList);
      }
      if (rs == null) {
        return null;
      }
      return rs.iterator();
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records from RightNowCloud; error - " + e.getMessage(), e);
    }
  }

  /**
   * Stream all files to extract resultSet
   * @return record set with each record as a JsonObject
   */
  private RecordSet<JsonElement> streamFiles(List<String> fileList)
      throws DataRecordException {
    log.info("Stream all jobs");
    RecordSetList<JsonElement> rs = new RecordSetList<>();
    try {
      //_currentReader.ready() will be false when there is nothing in _currentReader to be read
      if (_currentReader == null || !_currentReader.ready()) {
        if (_currentFileIndex >= fileList.size()) {
          log.info("Job is finished");
          _jobFinished = true;
          return rs;
        }

        String fileId = fileList.get(_currentFileIndex);
        log.debug("Current file Id:" + fileId);
        _currentReader =
            new BufferedReader(new InputStreamReader(this.getRequestAsStream(getEndPoint("file/" + fileId))));
        _currentFileIndex++;
      }

      InputStreamCSVReader reader = new InputStreamCSVReader(this._currentReader);

      if (_header == null) {
        _header = getHeader(reader.nextRecord());
        if (StringUtils.isBlank(workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA))) {
          List<String> timeStampColumns = Lists.newArrayList();
          String timeStampColumnString = this.workUnit.getProp(ZuoraConfigurationKeys.ZUORA_TIMESTAMP_COLUMNS);
          if (StringUtils.isNotBlank(timeStampColumnString)) {
            timeStampColumns = Arrays.asList(timeStampColumnString.toLowerCase().replaceAll(" ", "").split(","));
          }
          setSchema(_header, timeStampColumns);
        }
        log.info("record header:" + _header);
      }

      List<String> csvRecord;
      int recordCount = 0;
      while ((csvRecord = reader.nextRecord()) != null) {
        rs.add(Utils.csvToJsonObject(_header, csvRecord, _header.size()));
        totalRecordDownloaded++;
        recordCount++;
        if (recordCount >= _batchSize) {
          log.debug("Number of records in batch: " + recordCount);
          break;
        }
      }
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records from Zuora: " + e.getMessage(), e);
    }

    return rs;
  }

  private List<String> getFiles(String jobId)
      throws Exception {
    log.info("Get files for job " + jobId);
    String url = getEndPoint("batch-query/jobs/" + jobId);
    Command cmd = new RestApiCommand().build(Collections.singleton(url), RestApiCommandType.GET);

    String status = "pending";
    JsonObject jsonObject = null;
    while (!status.equals("completed")) {
      CommandOutput<?, ?> response = this.executeRequest(Collections.singletonList(cmd));
      Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
      if (!itr.hasNext()) {
        throw new DataRecordException("Failed to get data from RightNowCloud; REST response has no output");
      }
      String output = itr.next();
      log.info("Job " + jobId + " output: " + output);

      jsonObject = gson.fromJson(output, JsonObject.class).getAsJsonObject();
      status = jsonObject.get("status").getAsString();
      if (!status.equals("completed")) {
        log.info("Waiting for job to complete");
        Thread.sleep(5000);
      }
    }

    List<String> fileIds = Lists.newArrayList();
    for (JsonElement jsonObj : jsonObject.get("batches").getAsJsonArray()) {
      fileIds.add(jsonObj.getAsJsonObject().get("fileId").getAsString());
    }
    log.info("Files:" + fileIds);
    return fileIds;
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

  private String getEndPoint(String relativeUrl) {
    return workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME) + relativeUrl;
  }

  private static List<String> getHeader(ArrayList<String> cols) {
    List<String> columns = Lists.newArrayList();
    for (String col : cols) {
      String[] colRefs = col.split(":");
      String columnName;
      if (colRefs.length >= 2) {
        columnName = colRefs[1];
      } else {
        columnName = colRefs[0];
      }
      columns.add(columnName.replaceAll(" ", "").trim());
    }
    return columns;
  }
}
