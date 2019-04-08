/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.mapping;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;

/**
 * Vendor DatabaseId provider.
 * 厂商 DatabaseId 的提供者。
 *
 * It returns database product name as a databaseId.
 * If the user provides a properties it uses it to translate database product name
 * key="Microsoft SQL Server", value="ms" will return "ms".
 * It can return null, if no database product name or
 * a properties was specified and no translation was found.
 *
 * 它将数据库产品名称作为 databaseId 返回。
 * 如果用户提供了一个属性，它使用它来翻译数据库产品名称 key =“Microsoft SQL Server”，
 * 则value =“ms”将返回“ms”。 如果未指定数据库产品名称或属性且未找到转换，
 * 则它可以返回null。
 *
 * @author Eduardo Macarron
 */
public class VendorDatabaseIdProvider implements DatabaseIdProvider {

  /**
   * Properties 对象
   */
  private Properties properties;

  @Override
  public String getDatabaseId(DataSource dataSource) {
    if (dataSource == null) {
      throw new NullPointerException("dataSource cannot be null");
    }
    try {
      // 获得数据库标识
      return getDatabaseName(dataSource);
    } catch (Exception e) {
      LogHolder.log.error("Could not get a databaseId from dataSource", e);
    }
    return null;
  }

  @Override
  public void setProperties(Properties p) {
    this.properties = p;
  }

  private String getDatabaseName(DataSource dataSource) throws SQLException {
    // 获得数据库产品名
    String productName = getDatabaseProductName(dataSource);
    // 如果 properties 不为 null，则遍历 properties 查找 productName 对应的 value
    if (this.properties != null) {
      for (Map.Entry<Object, Object> property : properties.entrySet()) {
        // 如果产品名包含 KEY ，则返回对应的 VALUE
        if (productName.contains((String) property.getKey())) {
          return (String) property.getValue();
        }
      }
      // no match, return null
      return null;
    }
    // 不存在 properties ，则直接返回 productName
    return productName;
  }

  /**
   * 获得数据库产品名
   * @param dataSource 数据源
   * @return 数据库产品名
   * @throws SQLException
   */
  private String getDatabaseProductName(DataSource dataSource) throws SQLException {
    Connection con = null;
    try {
      // 获得数据库连接
      con = dataSource.getConnection();
      // 获取数据库元数据相关信息的接口
      DatabaseMetaData metaData = con.getMetaData();
      // 返回数据库产品名
      return metaData.getDatabaseProductName();
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (SQLException e) {
          // ignored
        }
      }
    }
  }

  private static class LogHolder {
    private static final Log log = LogFactory.getLog(VendorDatabaseIdProvider.class);
  }

}
