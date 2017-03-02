package gobblin.zuora;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.QueryBasedExtractor;
import gobblin.source.extractor.extract.SourceSpecificLayer;
import gobblin.source.extractor.extract.restapi.RestApiSpecificLayer;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.workunit.WorkUnit;


@Slf4j
public abstract class BasicRestApiExtractor extends QueryBasedExtractor<JsonArray, JsonElement> implements SourceSpecificLayer<JsonArray, JsonElement>, RestApiSpecificLayer {
  protected static final Gson gson = new Gson();

  public BasicRestApiExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public void closeConnection()
      throws Exception {
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws IOException {
    return null;
  }

  @Override
  public void setTimeOut(int timeOut) {

  }

  public boolean getPullStatus() {
    return true;
  }
}
