/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.models;


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
  Long idFrom,
  Tenant tenant
) {


  public QueryOptions(final String orderBy, final Boolean desc, final Instant creationDateFrom, final Instant creationDateTo, final Instant lastUpdateFrom, final Instant lastUpdateTo, final Integer pageNumber, final Integer pageSize, final Long idFrom, final Tenant tenant) {
    this.orderBy = orderBy;
    this.desc = desc;
    this.creationDateFrom = creationDateFrom;
    this.creationDateTo = creationDateTo;
    this.lastUpdateFrom = lastUpdateFrom;
    this.lastUpdateTo = lastUpdateTo;
    this.pageNumber = pageNumber;
    this.pageSize = pageSize;
    this.idFrom = idFrom;
    this.tenant = tenant;
  }

  public static QueryOptions simple(Tenant tenant) {
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
      tenant
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
      tenant
    );
  }

  public static QueryOptions from(final String orderBy, final RequestMetadata requestMetadata, final PublicQueryOptions options) {
    return new QueryOptions(
      orderBy,
      options.desc(),
      options.creationDateFrom(),
      options.creationDateTo(),
      options.lastUpdateFrom(),
      options.lastUpdateTo(),
      options.pageNumber(),
      options.pageSize(),
      null,
      requestMetadata.tenant()
    );
  }

}
