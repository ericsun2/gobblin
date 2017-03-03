package gobblin.zuora;

public class ZuoraConfigurationKeys {
  public static final String ZUORA_OUTPUT_FORMAT = "zuora.output.format";
  public static final String ZUORA_API_NAME = "zuora.api.name";
  public static final String ZUORA_PARTNER = "zuora.partner";
  public static final String ZUORA_PROJECT = "zuora.project";
  public static final String ZUORA_TIMESTAMP_COLUMNS = "zuora.timestamp.columns";
  public static final String ZUORA_ROW_LIMIT = "zuora.row.limit";

  public static final String ZUORA_API_RETRY_LIMIT_POST = "zuora.api.retry.limit.post";
  public static final String ZUORA_API_RETRY_WAIT_TIME_MILLIS_POST = "zuora.api.retry.wait.time.millis.post";
  public static final String ZUORA_API_RETRY_LIMIT_GET_FILES = "zuora.api.retry.limit.post.get.files";
  public static final String ZUORA_API_RETRY_WAIT_TIME_MILLIS_GET_FILES =
      "zuora.api.retry.wait.time.millis.post.get.files";
}
