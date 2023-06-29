package io.es4j.sql.generator.filters;

import java.util.List;
import java.util.StringJoiner;

public class ArrayColumnFilter {

    protected void arrayIn(StringJoiner stringJoiner, List<String> list, String column) {
        if (list != null && !list.isEmpty()) {
            final var searchArray = new StringJoiner(",");
            list.forEach(tag -> searchArray.add("'" + tag + "'::varchar"));
            stringJoiner.add(" array[" + searchArray + "] <@ " + column + " ");
        }
    }
}
