package io.eventx;

import io.eventx.commands.CreateData;
import io.vertx.core.json.JsonObject;

import java.lang.reflect.Field;

public class JsonGenerator {
  public static JsonObject generateJson(Class<?> recordClass) throws IllegalAccessException {
    JsonObject json = new JsonObject();
    for (Field field : recordClass.getDeclaredFields()) {
      field.setAccessible(true);
      String fieldName = field.getName();
      Class<?> fieldType = field.getType();

      JsonObject fieldJson = new JsonObject();
      fieldJson.put("type", fieldType.getName());
      json.put(fieldName, fieldJson);
    }

    return json;
  }

  // Example usage
  public static void main(String[] args) throws IllegalAccessException {
    JsonObject json = generateJson(CreateData.class);
    System.out.println(json.encodePrettily());
  }
}
