package gobblin.zuora;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

import javax.net.ssl.HttpsURLConnection;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.password.PasswordManager;


@Slf4j
public class ZuoraUtil {

  public static HttpsURLConnection getConnection(String urlPath, WorkUnitState workUnitState)
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
      String password =
          PasswordManager.getInstance(workUnitState).readPassword(workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
      String userpass = userName + ":" + password;
      String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
      connection.setRequestProperty("Authorization", basicAuth);
    }

    connection.setConnectTimeout(workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_TIMEOUT, 30000));
    return connection;
  }

  public static String getStringFromInputStream(InputStream is) {
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

  public static List<String> getHeader(ArrayList<String> cols) {
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
