package gobblin.zuora;

import java.io.BufferedReader;
import java.util.List;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.restapi.RestApiCommand;
import gobblin.source.extractor.watermark.Predicate;


public interface ZuoraClient {

  List<Command> buildPostCommand(List<Predicate> predicateList);

  CommandOutput<RestApiCommand, String> executePostRequest(final Command command)
      throws DataRecordException;

  List<String> getFileIds(String jobId)
      throws Exception;

  CommandOutput<RestApiCommand, String> executeGetRequest(final Command cmd)
      throws Exception;

  BufferedReader getFileBufferedReader(String fileId)
      throws Exception;
}
