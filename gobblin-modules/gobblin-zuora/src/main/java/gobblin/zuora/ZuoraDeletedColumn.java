package gobblin.zuora;

import java.io.Serializable;


public class ZuoraDeletedColumn implements Serializable {
  private static final long serialVersionUID = 1L;

  String column;

  ZuoraDeletedColumn(String columnName) {
    column = columnName;
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }
}
