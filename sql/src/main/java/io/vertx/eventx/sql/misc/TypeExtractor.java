package io.vertx.eventx.sql.misc;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class TypeExtractor {

    public static <T> Class<?> getActualGenericTypefromInterface(T object, Integer place) {
        Type[] genericInterfaces = object.getClass().getGenericInterfaces();
        final var genericInterface = genericInterfaces[0];
        if (genericInterface instanceof ParameterizedType parameterizedType) {
            Type[] genericTypes = parameterizedType.getActualTypeArguments();
//      LOGGER.debug(object.getClass().getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
            try {
                return Class.forName(genericTypes[place].getTypeName());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
            }
        } else {
            throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
        }
    }
}
