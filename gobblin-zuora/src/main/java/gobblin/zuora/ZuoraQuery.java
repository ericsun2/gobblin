package gobblin.zuora;

import java.io.Serializable;


public class ZuoraQuery implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  public String name;
  public String query;
  public String type = "zoqlexport";

  ZuoraQuery(String name, String query) {
    super();
    this.name = name;
    this.query = query;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

}
