package gobblin.zuora;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.exception.ExtractPrepareException;
import gobblin.source.extractor.extract.QueryBasedSource;


/**
 * An implementation of salesforce source to get work units
 */
public class ZuoraSource extends QueryBasedSource<JsonArray, JsonElement> {
  private static final Logger LOG = LoggerFactory.getLogger(QueryBasedSource.class);

  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state) throws IOException {
    Extractor<JsonArray, JsonElement> extractor = null;
    try {
      extractor = new ZuoraExtractor(state).build();
    } catch (ExtractPrepareException e) {
      LOG.error("Failed to prepare extractor: error - " + e.getMessage());
      throw new IOException(e);
    }
    return extractor;
  }
}
