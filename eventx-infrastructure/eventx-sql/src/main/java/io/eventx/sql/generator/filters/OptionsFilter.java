package io.eventx.sql.generator.filters;


import io.eventx.sql.models.QueryOptions;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.StringJoiner;

import static io.eventx.sql.misc.Constants.TENANT;


public class OptionsFilter {
  private OptionsFilter() {
  }

  public static void addOptionsQueryFiltersAndParams(QueryOptions options, StringJoiner queryFilters) {
    if (options.tenantId() != null) {
      queryFilters.add(" tenant = #{tenant} ");
    } else {
      queryFilters.add(" tenant like '%' ");
    }
    if (options.creationDateFrom() != null) {
      queryFilters.add(" creation_date >= #{creationDateFrom} ");
    }
    if (options.creationDateTo() != null) {
      queryFilters.add(" creation_date <= #{creationDateTo} ");
    }
    if (options.idFrom() != null) {
      queryFilters.add(" id >= " + options.idFrom() + " ");
    }
    if (options.lastUpdateFrom() != null) {
      queryFilters.add(" last_update >= #{lastUpdateDateFrom} ");
    }
    if (options.lastUpdateTo() != null) {
      queryFilters.add(" last_update <= #{lastUpdateDateTo} ");
    }
  }

  public static HashMap<String, Object> optionsParams(QueryOptions options) {
    final var fieldMap = new HashMap<String, Object>();
    if (options.creationDateFrom() != null) {
      fieldMap.put("creationDateFrom", LocalDateTime.ofInstant(options.creationDateFrom(), ZoneOffset.UTC));
    }
    if (options.creationDateTo() != null) {
      fieldMap.put("creationDateTo", LocalDateTime.ofInstant(options.creationDateTo(), ZoneOffset.UTC));
    }
    if (options.lastUpdateFrom() != null) {
      fieldMap.put("lastUpdateFrom", LocalDateTime.ofInstant(options.lastUpdateFrom(), ZoneOffset.UTC));
    }
    if (options.lastUpdateTo() != null) {
      fieldMap.put("lastUpdateTo", LocalDateTime.ofInstant(options.lastUpdateTo(), ZoneOffset.UTC));
    }
    if (options.tenantId() != null) {
      fieldMap.put(TENANT, options.tenantId());
    }
    if (options.pageSize() != null) {
      fieldMap.put("pageSize", options.pageSize());
    }
    if (options.pageSize() != null && options.pageNumber() != null) {
      fieldMap.put("offSet", options.pageSize() * options.pageNumber());
    }
    return fieldMap;
  }

  public static String limitAndOffset(QueryOptions options, boolean delete) {
    if (options.pageSize() != null && !delete) {
      if (options.pageNumber() != null) {
        return " offset #{offSet} fetch next #{pageSize} rows only;";
      }
      return " fetch first #{pageSize} rows only;";
    }
    if (delete) {
      return " returning *;";
    }
    return ";";
  }

  public static String getOrder(QueryOptions options, boolean delete) {
    if (options.orderBy() != null && !delete) {
      String orderStatement = " order by " + options.orderBy();
      if (Boolean.TRUE.equals(options.desc())) {
        orderStatement = orderStatement + " desc ";
      }
      return orderStatement;
    } else {
      return "";
    }
  }
}
