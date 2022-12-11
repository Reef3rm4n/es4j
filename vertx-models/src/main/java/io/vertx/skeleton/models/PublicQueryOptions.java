/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.models;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.vertx.core.json.JsonObject;

import javax.ws.rs.QueryParam;
import java.time.Instant;
import java.util.Objects;


@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class PublicQueryOptions {
  @QueryParam("desc")
  private boolean desc;
  @QueryParam("dateFrom")
  private Instant creationDateFrom;
  @QueryParam("dateTo")
  private Instant creationDateTo;
  @QueryParam("lastUpdateFrom")
  private Instant lastUpdateFrom;
  @QueryParam("lastUpdateTo")
  private Instant lastUpdateTo;
  @QueryParam("pageNumber")
  private Integer pageNumber;
  @QueryParam("pageSize")
  private Integer pageSize;

  public PublicQueryOptions() {
  }

  public PublicQueryOptions(JsonObject jsonObject) {
    final var object = jsonObject.mapTo(PublicQueryOptions.class);
    this.desc = object.desc();
    this.creationDateFrom = object.creationDateFrom();
    this.creationDateTo = object.creationDateTo();
    this.lastUpdateFrom = object.lastUpdateFrom();
    this.lastUpdateTo = object.lastUpdateTo();
    this.pageNumber = object.pageNumber();
    this.pageSize = object.pageSize();
  }

  public JsonObject toJson() {
    return JsonObject.mapFrom(this);
  }

  public PublicQueryOptions(@QueryParam("desc") final boolean desc, @QueryParam("dateFrom") final Instant creationDateFrom, @QueryParam("dateTo") final Instant creationDateTo, @QueryParam("lastUpdateFrom") final Instant lastUpdateFrom, @QueryParam("lastUpdateTo") final Instant lastUpdateTo, @QueryParam("pageNumber") final Integer pageNumber, @QueryParam("pageSize") final Integer pageSize) {
    this.desc = desc;
    this.creationDateFrom = creationDateFrom;
    this.creationDateTo = creationDateTo;
    this.lastUpdateFrom = lastUpdateFrom;
    this.lastUpdateTo = lastUpdateTo;
    this.pageNumber = pageNumber;
    this.pageSize = pageSize;
  }

  public static PublicQueryOptions simple() {
    return new PublicQueryOptions(
      false,
      null,
      null,
      null,
      null,
      null,
      null
    );
  }

  @QueryParam("desc")
  public boolean desc() {
    return desc;
  }

  @QueryParam("dateFrom")
  public Instant creationDateFrom() {
    return creationDateFrom;
  }

  @QueryParam("dateTo")
  public Instant creationDateTo() {
    return creationDateTo;
  }

  @QueryParam("lastUpdateFrom")
  public Instant lastUpdateFrom() {
    return lastUpdateFrom;
  }

  @QueryParam("lastUpdateTo")
  public Instant lastUpdateTo() {
    return lastUpdateTo;
  }

  @QueryParam("pageNumber")
  public Integer pageNumber() {
    return pageNumber;
  }

  @QueryParam("pageSize")
  public Integer pageSize() {
    return pageSize;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (PublicQueryOptions) obj;
    return this.desc == that.desc &&
      Objects.equals(this.creationDateFrom, that.creationDateFrom) &&
      Objects.equals(this.creationDateTo, that.creationDateTo) &&
      Objects.equals(this.lastUpdateFrom, that.lastUpdateFrom) &&
      Objects.equals(this.lastUpdateTo, that.lastUpdateTo) &&
      Objects.equals(this.pageNumber, that.pageNumber) &&
      Objects.equals(this.pageSize, that.pageSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(desc, creationDateFrom, creationDateTo, lastUpdateFrom, lastUpdateTo, pageNumber, pageSize);
  }

  @Override
  public String toString() {
    return "NotifierQueryOptions[" +
      "desc=" + desc + ", " +
      "creationDateFrom=" + creationDateFrom + ", " +
      "creationDateTo=" + creationDateTo + ", " +
      "lastUpdateFrom=" + lastUpdateFrom + ", " +
      "lastUpdateTo=" + lastUpdateTo + ", " +
      "pageNumber=" + pageNumber + ", " +
      "pageSize=" + pageSize + ']';
  }

  public PublicQueryOptions withPage(int pageNumber) {
    return new PublicQueryOptions(
      desc,
      creationDateFrom,
      creationDateTo,
      lastUpdateFrom,
      lastUpdateTo,
      pageNumber,
      pageSize
    );
  }
}
