package gobblin.zuora;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;

import com.google.gson.JsonElement;

import javax.net.ssl.HttpsURLConnection;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.resultset.RecordSet;
import gobblin.source.extractor.resultset.RecordSetList;
import gobblin.source.extractor.utils.InputStreamCSVReader;
import gobblin.source.extractor.utils.Utils;


@Slf4j
public class ZuoraClientFilesStreamer {
  private final String outputFormat;
  private final WorkUnitState _workUnitState;
  private final ZuoraClient _client;
  private final int batchSize;

  private boolean _jobFinished = false;
  private long _totalRecords = 0;

  private BufferedReader _currentReader;
  private int _currentFileIndex = -1;
  private int _skipHeaderIndex = 0; //Indicate whether the header has been skipped for a file.
  private HttpsURLConnection _currentConnection;

  public ZuoraClientFilesStreamer(WorkUnitState workUnitState, ZuoraClient client) {
    _workUnitState = workUnitState;
    _client = client;
    batchSize = workUnitState
        .getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE, ConfigurationKeys.DEFAULT_SOURCE_FETCH_SIZE);
    outputFormat = _workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_OUTPUT_FORMAT);
  }

  public RecordSet<JsonElement> streamFiles(List<String> fileList, List<String> header)
      throws DataRecordException {
    try {
      if (currentReaderDone()) {
        ++_currentFileIndex;
        closeCurrentSession();
        if (_currentFileIndex >= fileList.size()) {
          log.info("Finished streaming all files.");
          _jobFinished = true;
          return new RecordSetList<>();
        }
        initializeForNewFile(fileList);
      }
      log.info(String
          .format("Streaming file at index %s with id %s ...", _currentFileIndex, fileList.get(_currentFileIndex)));
      InputStreamCSVReader reader = new InputStreamCSVReader(_currentReader);
      if (_skipHeaderIndex == _currentFileIndex) {
        reader.nextRecord(); //skip header
        ++_skipHeaderIndex;
      }

      RecordSetList<JsonElement> rs = new RecordSetList<>();
      List<String> csvRecord;
      int count = 0;
      while ((csvRecord = reader.nextRecord()) != null) {
        rs.add(Utils.csvToJsonObject(header, csvRecord, header.size()));
        ++_totalRecords;
        if (++count >= batchSize) {
          break;
        }
      }
      log.info("Total number of records downloaded: " + _totalRecords);
      return rs;
    } catch (Exception e) {
      try {
        closeCurrentSession();
      } catch (IOException e1) {
        log.error(e1.getMessage());
      }
      throw new DataRecordException("Failed to get records from Zuora: " + e.getMessage(), e);
    }
  }

  private void initializeForNewFile(List<String> fileList)
      throws IOException {
    String fileId = fileList.get(_currentFileIndex);
    log.info(String.format("Start streaming file at index %s with id %s", _currentFileIndex, fileId));
    _currentConnection = ZuoraUtil.getConnection(_client.getEndPoint("file/" + fileId), _workUnitState);
    _currentConnection.setRequestProperty("Accept", "application/json");
    InputStream stream = _currentConnection.getInputStream();
    if (StringUtils.isNotBlank(outputFormat) && outputFormat.equalsIgnoreCase("gzip")) {
      stream = new GZIPInputStream(stream);
    }
    _currentReader = new BufferedReader(new InputStreamReader(stream));
  }

  private void closeCurrentSession()
      throws IOException {
    if (_currentConnection != null) {
      _currentConnection.disconnect();
    }
    if (_currentReader != null) {
      _currentReader.close();
    }
  }

  private boolean currentReaderDone()
      throws IOException {
    //_currentReader.ready() will be false when there is nothing in _currentReader to be read
    return _currentReader == null || !_currentReader.ready();
  }

  public boolean isJobFinished() {
    return _jobFinished;
  }
}
