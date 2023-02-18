package io.vertx.skeleton.sql.generator.filters;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.StringJoiner;

public class RangeFilter {

    private RangeFilter(){}
    public static void addFromFilter(StringJoiner stringJoiner, String column, Object field) {
        if (field instanceof Instant instant) {
            instantFrom(stringJoiner, instant, column);
        } else if (field instanceof LocalDateTime localDateTime) {
            dateTimeFrom(stringJoiner, localDateTime, column);
        } else if (field instanceof Long lNumber) {
            from(stringJoiner, lNumber, column);
        } else if (field instanceof Integer iNumber) {
            from(stringJoiner, iNumber, column);
        } else if (field instanceof LocalTime localTime) {
            // todo
        }
    }

    public static void instantFrom(StringJoiner stringJoiner, Instant dateFrom, String column) {
        if (dateFrom != null) {
            stringJoiner.add(" " + column + " >= '" + LocalDateTime.ofInstant(dateFrom, ZoneOffset.UTC) + "'::timestamp ");
        }
    }

    public static void dateTimeFrom(StringJoiner stringJoiner, LocalDateTime dateFrom, String column) {
        if (dateFrom != null) {
            stringJoiner.add(" " + column + " >= '" + dateFrom + "'::timestamp ");
        }
    }

    public static void from(StringJoiner joiner, Integer integer, String column) {
        if (integer != null) {
            joiner.add(" " + column + " >= " + integer + " ");
        }

    }

    public static void from(StringJoiner joiner, Long aLong, String column) {
        if (aLong != null) {
            joiner.add(" " + column + " >= " + aLong + " ");
        }

    }

    public static void addToFilter(StringJoiner stringJoiner, String column, Object field) {
        if (field instanceof Instant instant) {
            dateTo(stringJoiner, instant, column);
        } else if (field instanceof LocalDateTime localDateTime) {
            dateTo(stringJoiner, localDateTime, column);
        } else if (field instanceof Long lNumber) {
            to(stringJoiner, lNumber, column);
        } else if (field instanceof Integer iNumber) {
            to(stringJoiner, iNumber, column);
        } else if (field instanceof LocalTime localTime) {
            // todo
        }
    }

    protected static void dateTo(StringJoiner stringJoiner, Instant dateFrom, String column) {
        if (dateFrom != null) {
            stringJoiner.add(" " + column + " <= '" + LocalDateTime.ofInstant(dateFrom, ZoneOffset.UTC) + "'::timestamp ");
        }
    }

    protected static void dateTo(StringJoiner stringJoiner, LocalDateTime dateFrom, String column) {
        if (dateFrom != null) {
            stringJoiner.add(" " + column + " <= '" + dateFrom + "'::timestamp ");
        }
    }

    protected static void to(StringJoiner joiner, Integer integer, String column) {
        if (integer != null) {
            joiner.add(" " + column + " <= " + integer + " ");
        }
    }


    protected static void to(StringJoiner joiner, Long aLong, String column) {
        if (aLong != null) {
            joiner.add(" " + column + " <= " + aLong + " ");
        }

    }
}
