/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.es4j.core.objects;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;


@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@RecordBuilder
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
