package io.eventx.http;

import com.google.auto.service.AutoService;
import io.smallrye.mutiny.tuples.Tuple2;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

import static io.eventx.core.CommandHandler.camelToKebab;


@AutoService(javax.annotation.processing.Processor.class)
public class OpenApiGenerator extends AbstractProcessor {

  private static boolean generated = false;

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    Set<String> supportedAnnotations = new HashSet<>();
    supportedAnnotations.add(OpenApiDocs.class.getCanonicalName());
    return supportedAnnotations;
  }

  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    Map<TypeMirror, List<TypeMirror>> typeArgumentsMap = new LinkedHashMap<>();
    roundEnv.getElementsAnnotatedWith(OpenApiDocs.class).forEach(
      element -> {
        if (element.getKind() == ElementKind.CLASS) {
          TypeElement typeElement = (TypeElement) element;

          // Iterate through the interfaces implemented by the class
          for (TypeMirror interfaceTypeMirror : typeElement.getInterfaces()) {
            if (interfaceTypeMirror instanceof DeclaredType) {
              DeclaredType declaredType = (DeclaredType) interfaceTypeMirror;
              TypeElement interfaceElement = (TypeElement) declaredType.asElement();

              if ("Behaviour".equals(interfaceElement.getSimpleName().toString())) {
                List<? extends TypeMirror> typeArguments = declaredType.getTypeArguments();
                TypeMirror typeArgumentA = typeArguments.get(0);
                TypeMirror typeArgumentC = typeArguments.get(1);
                final var list = typeArgumentsMap.getOrDefault(typeArgumentA, new ArrayList<>());
                list.add(typeArgumentC);
                typeArgumentsMap.put(typeArgumentA, list);
              }
            }
          }
        }
      }
    );
    if (!generated) {
      System.out.println(typeArgumentsMap);
      final var interfaceSource = generateJavaInterfaceWithSwagger(typeArgumentsMap);
//      final var implementationSource = generateJavaInterfaceImplementation(typeArgumentsMap);
      interfaceSource.forEach(
        tuple -> writeFile(tuple.getItem1(), tuple.getItem2())
      );
//      implementationSource.forEach(
//        tuple -> writeFile(tuple.getItem1(), tuple.getItem2())
//      );
      generated = true;
      return true;
    }
    return false;
  }

  private void writeFile(String className, String sourceCode) {
    try {
      JavaFileObject file = processingEnv.getFiler().createSourceFile(className);
      try (Writer writer = file.openWriter()) {
        writer.write(sourceCode);
      }
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Failed to write class: " + e.getMessage());
    }
  }

  private List<Tuple2<String, String>> generateJavaInterfaceWithSwagger(Map<TypeMirror, List<TypeMirror>> typeArgumentsMap) {
    final var interfaces = new ArrayList<Tuple2<String, String>>();
    typeArgumentsMap.forEach(
      (aggregate, value) -> {
        StringBuilder builder = new StringBuilder();
        final var aggregateTypeElement = (TypeElement) processingEnv.getTypeUtils().asElement(aggregate);
        final var aggregateSimpleName = aggregateTypeElement.getSimpleName().toString();
        // Package
        builder.append(convertToPackageStatement(aggregate.toString()) + "\n\n");

        // Imports
        builder.append("import org.eclipse.microprofile.openapi.annotations.*;\n");
        builder.append("import org.eclipse.microprofile.openapi.annotations.media.*;\n");
        builder.append("import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;\n");
        builder.append("import org.eclipse.microprofile.openapi.annotations.tags.*;\n");
        builder.append("import org.eclipse.microprofile.openapi.annotations.info.*;\n");
        builder.append("import javax.ws.rs.*;\n");
        builder.append("import javax.ws.rs.core.MediaType;\n");
        builder.append("\n");

        builder.append("@Produces(MediaType.APPLICATION_JSON)\n");
        builder.append("@Consumes(MediaType.APPLICATION_JSON)\n");
        builder.append("@OpenAPIDefinition(\n" +
          "  info = @Info(\n" +
          "    title = \"" + aggregateSimpleName + "\",\n" +
          "    version = \"1.0.0\"\n" +
          "  )\n" +
          ")\n");
        builder.append("@Path(\"/" + camelToKebab(aggregateSimpleName) + "\")\n");

        // Interface declaration
        builder.append("public interface " + aggregateSimpleName + "Api {\n\n");
        value.forEach(
          command -> {
            final var commandTypeElement = (TypeElement) processingEnv.getTypeUtils().asElement(command);
            final var commandSimpleName = commandTypeElement.getSimpleName().toString();
            builder.append("    @POST\n");
            builder.append("    @Path(\"/").append(camelToKebab(commandSimpleName)).append("\")\n");
            builder.append("    @Operation(summary = \"Process command\", description = \"Processes a command\")\n");
            builder.append("    @APIResponse(responseCode = \"200\", description = \"Success\", content = @Content(schema = @Schema(implementation = ").append(aggregate).append(".class)))\n");
            builder.append("    ").append("default io.eventx.core.objects.AggregateState<").append(aggregate).append("> " + camelToSnake(commandSimpleName) + "(").append(command).append(" command){return null;}\n\n");
          }
        );

        builder.append("}\n");
        interfaces.add(Tuple2.of(aggregateSimpleName + "Api", builder.toString()));
      }
    );
    return interfaces;
  }

  private List<Tuple2<String, String>> generateJavaInterfaceImplementation(Map<TypeMirror, List<TypeMirror>> typeArgumentsMap) {
    final var interfaces = new ArrayList<Tuple2<String, String>>();
    typeArgumentsMap.forEach(
      (aggregate, commandList) -> {
        StringBuilder builder = new StringBuilder();
        final var aggregateTypeElement = (TypeElement) processingEnv.getTypeUtils().asElement(aggregate);
        final var aggregateSimpleName = aggregateTypeElement.getSimpleName().toString();
        // Package
        builder.append(convertToPackageStatement(aggregate.toString()) + "\n\n");

        // Imports
        builder.append("import javax.ws.rs.*;\n");

        // Class declaration
        builder.append("public class " + aggregateSimpleName + "Impl implements " + extractPackageStatement(aggregate.toString()) + "." + aggregateSimpleName + "Api {\n\n");
        commandList.forEach(
          command -> {
            final var commandTypeElement = (TypeElement) processingEnv.getTypeUtils().asElement(command);
            final var commandSimpleName = commandTypeElement.getSimpleName().toString();
            builder.append("    @Override\n");
            builder.append("    public ").append(aggregate).append(" " + camelToSnake(commandSimpleName) + "(").append(command).append(" command) {\n");
            builder.append("        return null;\n");
            builder.append("    }\n\n");
          }
        );
        builder.append("}\n");
        interfaces.add(Tuple2.of(aggregate + "Impl", builder.toString()));
      }
    );
    return interfaces;
  }

  public static String camelToSnake(String str) {
    // Regular Expression
    String regex = "([a-z])([A-Z]+)";

    // Replacement string
    String replacement = "$1_$2";

    // Replace the given regex
    // with replacement string
    // and convert it to lower case.
    str = str
      .replaceAll(
        regex, replacement)
      .toLowerCase();

    // return string
    return str;
  }

  public static String convertToPackageStatement(String className) {
    int lastDotIndex = className.lastIndexOf('.');
    if (lastDotIndex != -1) {
      String packageName = className.substring(0, lastDotIndex);
      return "package " + packageName + ";";
    } else {
      return "";
    }
  }

  public static String extractPackageStatement(String className) {
    int lastDotIndex = className.lastIndexOf('.');
    if (lastDotIndex != -1) {
      return className.substring(0, lastDotIndex);
    } else {
      return "";
    }
  }

}

