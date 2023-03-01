package io.vertx.eventx.sql.models;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.vertx.core.json.JsonObject;

import java.io.Serializable;
import java.util.Objects;


@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class CobraError implements Serializable {
    private String cause;
    private String hint;
    private Integer errorCode;

    public CobraError(
        String cause,
        String hint,
        Integer errorCode
    ) {
        this.cause = cause;
        this.hint = hint;
        this.errorCode = errorCode;
    }

    public CobraError() {
    }

    public CobraError(final JsonObject jsonObject) {
        final var object = jsonObject.mapTo(CobraError.class);
        this.cause = object.cause();
        this.hint = object.hint();
        this.errorCode = object.errorCode();
    }

    public JsonObject toJson() {
        return JsonObject.mapFrom(this);
    }

    public String cause() {
        return cause;
    }

    public String hint() {
        return hint;
    }

    public Integer errorCode() {
        return errorCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (CobraError) obj;
        return Objects.equals(this.cause, that.cause) &&
            Objects.equals(this.hint, that.hint) &&
            Objects.equals(this.errorCode, that.errorCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cause, hint, errorCode);
    }

    @Override
    public String toString() {
        return "Error[" +
            "cause=" + cause + ", " +
            "hint=" + hint + ", " +
            "errorCode=" + errorCode + ']';
    }


}
