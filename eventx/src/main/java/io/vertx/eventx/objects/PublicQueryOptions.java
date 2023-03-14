/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.eventx.objects;


import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.time.Instant;


@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record PublicQueryOptions(
   boolean desc,
   Instant creationDateFrom,
   Instant creationDateTo,
   Instant lastUpdateFrom,
   Instant lastUpdateTo,
   Integer pageNumber,
   Integer pageSize
) {

}
