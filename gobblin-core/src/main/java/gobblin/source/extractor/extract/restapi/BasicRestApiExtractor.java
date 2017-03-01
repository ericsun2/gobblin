package gobblin.source.extractor.extract.restapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import javax.net.ssl.HttpsURLConnection;
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
import gobblin.source.extractor.extract.SourceSpecificLayer;
import gobblin.source.extractor.extract.restapi.RestApiCommand.RestApiCommandType;
import gobblin.source.extractor.schema.ColumnNameCase;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.workunit.WorkUnit;


@Slf4j
public abstract class BasicRestApiExtractor extends QueryBasedExtractor<JsonArray, JsonElement> implements SourceSpecificLayer<JsonArray, JsonElement>, RestApiSpecificLayer {
  protected static final Gson gson = new Gson();
  protected long processedRecordCount = 0;
  private boolean _firstRun = true;

  public long getProcessedRecordCount() {
    return processedRecordCount;
  }

  public void setProcessedRecordCount(long processedRecordCount) {
    this.processedRecordCount = processedRecordCount;
  }

  public boolean getPullStatus() {
    return true;
  }

  public BasicRestApiExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public void extractMetadata(String schema, String entity, WorkUnit workUnit)
      throws SchemaException, IOException {
    // TODO Auto-generated method stub
    log.info("Extract Metadata using REST Api");
    JsonArray columnArray = new JsonArray();
    JsonArray array = null;
    try {
      List<Command> cmds = this.getSchemaMetadata(schema, entity);
      CommandOutput<?, ?> response = null;
      if (cmds != null) {
        response = this.executeGetRequest(cmds);
      }
      array = this.getSchema(response);
      if (array == null) {
        log.warn("Schema not found in metadata and configurations");
        columnArray = this.getDefaultSchema();
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
    long CalculatedHighWatermark = -1;
    try {
      List<Command> cmds = this.getHighWatermarkMetadata(schema, entity, watermarkColumn, snapshotPredicateList);
      CommandOutput<?, ?> response = this.executeRequest(cmds);
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
    long count = 0;
    try {
      List<Command> cmds = this.getCountMetadata(schema, entity, workUnit, predicateList);
      CommandOutput<?, ?> response = this.executeRequest(cmds);
      count = this.getCount(response);
    } catch (Exception e) {
      throw new RecordCountException("Failed to get record count using rest api; error - " + e.getMessage(), e);
    }
    if (count != 0) {
      return count;
    }
    return this.getProcessedRecordCount();
  }

  @Override
  public Iterator<JsonElement> getRecordSet(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws DataRecordException, IOException {
    Iterator<JsonElement> rs;
    CommandOutput<?, ?> response = null;
    try {
      if (!this.getPullStatus()) {
        log.info("pull status false");
        return null;
      } else {
        if (_firstRun) {
          List<Command> cmds = this.getDataMetadata(schema, entity, workUnit, predicateList);
          response = this.executePostRequest(cmds);
        }
        rs = this.getData(response);
        log.info("Total number of records processed - " + this.processedRecordCount);
        _firstRun = false;
      }
      return rs;
    } catch (Exception e) {
      e.printStackTrace();
      throw new DataRecordException("Failed to get records using rest API; error - " + e.getMessage(), e);
    }
  }

  protected CommandOutput<?, ?> executeRequest(List<Command> cmds)
      throws Exception {
    if (cmds == null || cmds.isEmpty()) {
      return null;
    }
    RestApiCommandType commandType = (RestApiCommandType) cmds.get(0).getCommandType();
    CommandOutput<?, ?> output = null;
    switch (commandType) {
      case GET:
        output = executeGetRequest(cmds);
        break;
      case POST:
        output = executePostRequest(cmds);
        break;
      default:
        log.error("Invalid REST API command type " + commandType);
        break;
    }
    return output;
  }

  private CommandOutput<?, ?> executeGetRequest(List<Command> cmds)
      throws Exception {
    HttpsURLConnection connection = null;
    String urlPath = cmds.get(0).getParams().get(0);
    String result = null;
    try {
      connection = getConnection(urlPath, workUnitState);
      connection.setRequestProperty("Accept", "application/json");

      InputStream in = connection.getInputStream();
      result = getStringFromInputStream(in);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      log.error("failed to open stream for schema");
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
    CommandOutput<RestApiCommand, String> output = new RestApiCommandOutput();
    output.put((RestApiCommand) cmds.get(0), result);
    return output;
  }

  protected InputStream getRequestAsStream(String urlPath)
      throws Exception {
    HttpsURLConnection connection = null;
    InputStream stream = null;
    try {
      connection = getConnection(urlPath, workUnitState);
      connection.setRequestProperty("Accept", "application/json");
      stream = connection.getInputStream();
      if (isZipFormat()) {
        stream = new GZIPInputStream(stream);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      log.error("failed to open stream for schema");
    }
    return stream;
  }

  private boolean isZipFormat() {
    String format = this.workUnitState.getProp(RestAPIConfigurationKeys.REST_API_OUTPUT_FORMAT);
    if (StringUtils.isNotBlank(format) && format.equalsIgnoreCase("gzip")) {
      return true;
    }
    return false;
  }

  private CommandOutput<?, ?> executePostRequest(List<Command> cmds)
      throws Exception {
    HttpsURLConnection connection = null;
    List<String> params = cmds.get(0).getParams();

    CommandOutput<RestApiCommand, String> output = new RestApiCommandOutput();
    int retryLimit = this.workUnitState.getPropAsInt(RestAPIConfigurationKeys.REST_API_RETRY_LIMIT, 3);
    int retryCount = 1;
    boolean isFinished = false;
    BufferedReader br = null;
    while (retryCount <= retryLimit && !isFinished) {
      try {
        String payLoad = params.get(1);
        log.info("payLoad:" + payLoad);

        connection = getConnection(params.get(0), workUnitState);
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");

        OutputStream os = connection.getOutputStream();
        os.write(payLoad.getBytes());
        os.flush();

        br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = br.readLine()) != null) {
          result.append(line);
        }
        output.put((RestApiCommand) cmds.get(0), result.toString());
        isFinished = true;
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        e.printStackTrace();
        log.warn("Retrying request to extract data; error - " + e.getMessage());
        retryCount++;
        Thread.sleep(this.workUnitState.getPropAsInt(RestAPIConfigurationKeys.REST_API_RETRY_WAIT_TIME_MILLIS, 10000));
      } finally {
        if (br != null) {
          br.close();
        }
        if (connection != null) {
          connection.disconnect();
        }
      }
    }

    if (retryCount >= retryLimit) {
      log.error("Failed to extract data after " + retryLimit + " attempts");
    }

    return output;
  }

  private String getStringFromInputStream(InputStream is) {
    BufferedReader br = null;
    StringBuilder sb = new StringBuilder();
    String line;
    try {
      br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return sb.toString();
  }

  protected List<Command> constructGetCommand(String restQuery) {
    return Arrays.asList(new RestApiCommand().build(Arrays.asList(restQuery), RestApiCommandType.GET));
  }

  protected String toCase(String targetColumnName) {
    String columnName = targetColumnName;
    ColumnNameCase caseType = ColumnNameCase.valueOf(this.workUnitState
        .getProp(ConfigurationKeys.SOURCE_COLUMN_NAME_CASE, ConfigurationKeys.DEFAULT_COLUMN_NAME_CASE).toUpperCase());
    switch (caseType) {
      case TOUPPER:
        columnName = targetColumnName.toUpperCase();
        break;
      case TOLOWER:
        columnName = targetColumnName.toLowerCase();
        break;
      default:
        columnName = targetColumnName;
        break;
    }
    return columnName;
  }

  @Override
  public void closeConnection()
      throws Exception {
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws IOException {
    //This operation is not supported
    return null;
  }

  @Override
  public void setTimeOut(int timeOut) {

  }

  public void setSchema(List<String> cols, List<String> timestampColumns) {
    JsonArray columnArray = new JsonArray();
    for (String columnName : cols) {
      Schema obj = new Schema();
      obj.setColumnName(columnName);
      obj.setComment("resolved");
      obj.setWaterMark(
          this.isWatermarkColumn(workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY), columnName));

      if (this.isWatermarkColumn(workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY), columnName)) {
        obj.setNullable(false);
        obj.setDataType(this.getTimestampDataType(columnName));
      } else if (this.getPrimarykeyIndex(workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY), columnName)
          == 0) {
        // set all columns as nullable except primary key and watermark columns
        obj.setNullable(true);
      }

      if (timestampColumns != null && timestampColumns.contains(columnName.toLowerCase())) {
        obj.setDataType(this.getTimestampDataType(columnName));
      }

      obj.setPrimaryKey(
          this.getPrimarykeyIndex(workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY), columnName));

      String jsonStr = gson.toJson(obj);
      JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
      columnArray.add(jsonObject);
    }

    log.info("Resolved Schema:" + columnArray);
    this.setOutputSchema(columnArray);
  }

  public void setSchema(List<String> cols) {
    this.setSchema(cols, null);
  }

  private JsonObject getTimestampDataType(String columnName) {
    JsonObject newDataType = this.convertDataType(columnName, "timestamp", null, null);
    return newDataType;
  }

  private JsonArray getDefaultSchema() {
    JsonArray columnArray = new JsonArray();
    String pk = this.workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
    if (StringUtils.isNotBlank(pk)) {
      List<String> pkCols = Arrays.asList(pk.replaceAll(" ", "").split(","));
      for (String col : pkCols) {
        Schema obj = new Schema();
        obj.setColumnName(col);
        obj.setDataType(this.convertDataType(col, null, null, null));
        obj.setComment("default");
        String jsonStr = gson.toJson(obj);
        JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        columnArray.add(jsonObject);
      }
    }

    String watermark = this.workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
    if (StringUtils.isNotBlank(watermark)) {
      List<String> watermarkCols = Arrays.asList(watermark.replaceAll(" ", "").split(","));
      for (String col : watermarkCols) {
        Schema obj = new Schema();
        obj.setColumnName(col);
        obj.setDataType(this.convertDataType(col, null, null, null));
        obj.setComment("default");
        obj.setWaterMark(true);
        String jsonStr = gson.toJson(obj);
        JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        columnArray.add(jsonObject);
      }
    }
    return columnArray;
  }

  private static HttpsURLConnection getConnection(String urlPath, WorkUnitState workUnitState)
      throws IOException {
    log.info("URL: " + urlPath);

    URL url = new URL(urlPath);
    HttpsURLConnection connection;
    String proxyUrl = workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL);
    if (StringUtils.isNotBlank(proxyUrl)) {
      int proxyPort = workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT);
      Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUrl, proxyPort));
      connection = (HttpsURLConnection) url.openConnection(proxy);
    } else {
      connection = (HttpsURLConnection) url.openConnection();
    }

    connection.setRequestProperty("Content-Type", "application/json");

    String userName = workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
    if (StringUtils.isNotBlank(userName)) {
      String userpass = userName + ":" + workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD);
      String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
      connection.setRequestProperty("Authorization", basicAuth);
    }

    connection.setConnectTimeout(workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_TIMEOUT, 30000));
    return connection;
  }
}
