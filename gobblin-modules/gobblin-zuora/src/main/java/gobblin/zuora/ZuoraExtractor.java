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
  private int currentFileNum = 0;
  private boolean newFile = true;
  private List<String> recordHeader = null;
  protected boolean jobFinished = false;

  public boolean isJobFinished() {
    return this.jobFinished;
  }

  public void setJobFinished(boolean jobFinished) {
    this.jobFinished = jobFinished;
  }

  public List<String> getRecordHeader() {
    return recordHeader;
  }

  public void setRecordHeader(List<String> recordHeader) {
    this.recordHeader = recordHeader;
  }

  public boolean isNewFile() {
    return newFile;
  }

  public void setNewFile(boolean newFile) {
    this.newFile = newFile;
  }

  public ZuoraExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
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
    log.debug("Build url to retrieve data records");
    try {
      String host = getEndPoint("batch-query/");
      List<String> params = Lists.newLinkedList();
      params.add(host);

      String query = this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY);
      if (StringUtils.isBlank(query)) {
        query = "SELECT * FROM " + this.workUnitState.getProp(ConfigurationKeys.SOURCE_ENTITY);
      }

      String limitString = getLimitFromInputQuery(query);
      if (StringUtils.isNotBlank(limitString)) {
        query = query.replace(limitString, "");
      }

      if (predicateList != null) {
        for (Predicate predicate : predicateList) {
          query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
        }
      }

      if (StringUtils.isBlank(limitString) && StringUtils
          .isNotBlank(this.workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_ROW_LIMIT))) {
        limitString = " LIMIT " + this.workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_ROW_LIMIT);
      }

      query = query + limitString;

      List<ZuoraQuery> queries = Lists.newArrayList();
      queries.add(new ZuoraQuery(this.workUnitState.getProp(ConfigurationKeys.JOB_NAME_KEY), query));
      ZuoraParams filterPayload =
          new ZuoraParams(this.workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_PARTNER, "sample"),
              this.workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_PROJECT, "sample"), queries,
              this.workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_API_NAME, "sample"),
              this.workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_OUTPUT_FORMAT, "csv"),
              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_VERSION, "1.1"));
      params.add(gson.toJson(filterPayload));

      return Arrays.asList(new RestApiCommand().build(params, RestApiCommandType.PUT));
    } catch (Exception e) {
      throw new DataRecordException("Failed to get RightNowCloud url for data records; error - " + e.getMessage(), e);
    }
  }

  private static String getLimitFromInputQuery(String query) {
    String inputQuery = query.toLowerCase();
    int limitIndex = inputQuery.indexOf(" limit");
    if (limitIndex > 0) {
      return query.substring(limitIndex);
    }
    return "";
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
      if (!this.isJobFinished()) {
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

  BufferedReader bufferedReader;
  /**
   * Stream all files to extract resultSet
   * @return record set with each record as a JsonObject
   */
  private RecordSet<JsonElement> streamFiles(List<String> fileList)
      throws DataRecordException {
    log.info("Stream all jobs");
    RecordSetList<JsonElement> rs = new RecordSetList<>();
    try {
      if (bufferedReader == null || !bufferedReader.ready()) {
        if (currentFileNum < fileList.size()) {
          log.debug("Stream resultset for resultId:" + fileList.get(currentFileNum));
          bufferedReader = new BufferedReader(
              new InputStreamReader(this.getRequestAsStream(getEndPoint("file/" + fileList.get(currentFileNum)))));
          //          this.printBuffer(); //Buffer will be empty after this
          currentFileNum++;
          setNewFile(true);
        } else {
          // Mark the job as finished if all files are processed
          log.info("Job is finished");
          this.setJobFinished(true);
          return rs;
        }
      }

      int batchSize = this.workUnit
          .getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE, ConfigurationKeys.DEFAULT_SOURCE_FETCH_SIZE);

      // Stream the resultset through CSV reader to identify columns in each record
      InputStreamCSVReader reader = new InputStreamCSVReader(this.bufferedReader);

      // Get header for the first file only
      if (this.isNewFile()) {
        List<String> recordHeader = this.cleanHeader(reader.nextRecord());
        List<String> timestampColumns = Lists.newArrayList();
        if (StringUtils.isNotBlank(this.workUnit.getProp(ZuoraConfigurationKeys.ZUORA_TIMESTAMP_COLUMNS))) {
          timestampColumns = Arrays.asList(
              this.workUnit.getProp(ZuoraConfigurationKeys.ZUORA_TIMESTAMP_COLUMNS).toLowerCase().replaceAll(" ", "")
                  .split(","));
        }

        if (StringUtils.isBlank(this.workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA))) {
          this.setSchema(recordHeader, timestampColumns);
        }

        this.setRecordHeader(recordHeader);
        this.setNewFile(false);

        log.info("record header:" + getRecordHeader());
      }

      List<String> csvRecord;
      int recordCount = 0;

      // Get record from CSV reader stream
      while ((csvRecord = reader.nextRecord()) != null) {
        // Convert CSV record to JsonObject
        JsonObject jsonObject = Utils.csvToJsonObject(this.getRecordHeader(), csvRecord, this.getRecordHeader().size());
        rs.add(jsonObject);
        this.processedRecordCount++;
        recordCount++;
        // Insert records in record set until it reaches the batch size
        if (recordCount >= batchSize) {
          log.debug("Number of records in batch: " + recordCount);
          break;
        }
      }
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records from salesforce; error - " + e.getMessage(), e);
    }

    return rs;
  }

  private List<String> cleanHeader(ArrayList<String> cols) {
    List<String> columns = Lists.newArrayList();
    for (String col : cols) {
      String[] colRefs = col.split(":");
      String columnName = null;
      if (colRefs.length >= 2) {
        columnName = colRefs[1];
      } else {
        columnName = colRefs[0];
      }
      columns.add(columnName.replaceAll(" ", "").trim());
    }
    return columns;
  }

  protected void printBuffer()
      throws IOException {
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      log.info("line:" + line);
    }
  }

  private String getEndPoint(String relativeUrl) {
    return this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME) + relativeUrl;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNextUrl() {
    // TODO Auto-generated method stub
    return null;
  }
}
