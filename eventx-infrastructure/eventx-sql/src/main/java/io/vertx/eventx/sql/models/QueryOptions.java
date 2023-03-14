package io.vertx.eventx.sql.models;


import java.time.Instant;

public record QueryOptions(
    String orderBy,
    Boolean desc,
    Instant creationDateFrom,
    Instant creationDateTo,
    Instant lastUpdateFrom,
    Instant lastUpdateTo,
    Integer pageNumber,
    Integer pageSize,
    String idFrom,
    String tenantId
) {


    public QueryOptions(final String orderBy, final Boolean desc, final Instant creationDateFrom, final Instant creationDateTo, final Instant lastUpdateFrom, final Instant lastUpdateTo, final Integer pageNumber, final Integer pageSize, final String idFrom, final String tenantId) {
        this.orderBy = orderBy;
        this.desc = desc;
        this.creationDateFrom = creationDateFrom;
        this.creationDateTo = creationDateTo;
        this.lastUpdateFrom = lastUpdateFrom;
        this.lastUpdateTo = lastUpdateTo;
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.idFrom = idFrom;
        this.tenantId = tenantId;
    }

    public static QueryOptions simple(String tenantId) {
        return new QueryOptions(
            null,
            null,
            null,
            null,
            null,
            null,
            0,
            100,
            null,
            tenantId
        );
    }

  public static QueryOptions simple() {
    return new QueryOptions(
      null,
      null,
      null,
      null,
      null,
      null,
      0,
      100,
      null,
      null
    );
  }

    public QueryOptions withPage(int pageNumber) {
        return new QueryOptions(
            orderBy,
            desc,
            creationDateFrom,
            creationDateTo,
            lastUpdateFrom,
            lastUpdateTo,
            pageNumber,
            pageSize,
            idFrom,
            tenantId
        );
    }


}
