//package io.eventx.config;
//
//import io.eventx.config.orm.ConfigurationKey;
//import io.eventx.config.orm.ConfigurationQuery;
//import io.eventx.config.orm.ConfigurationRecord;
//import io.eventx.sql.Repository;
//import io.eventx.sql.models.BaseRecord;
//import io.eventx.sql.models.QueryOptionsBuilder;
//import io.smallrye.mutiny.Multi;
//import io.smallrye.mutiny.Uni;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import io.vertx.mutiny.core.Vertx;
//import io.vertx.mutiny.core.buffer.Buffer;
//import io.vertx.mutiny.ext.web.FileUpload;
//import io.vertx.mutiny.ext.web.Router;
//
//import java.util.Objects;
//
//public class ConfigurationRoute implements HttpRoute {
//
//  private final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> configuration;
//  private final Vertx vertx;
//
//  public ConfigurationRoute(Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> configuration) {
//    this.configuration = configuration;
//    this.vertx = configuration.repositoryHandler().vertx();
//  }
//
//
//  @Override
//  public void registerRoutes(Router router) {
//    router.get("/configuration/download/:tenant")
//      .handler(routingContext -> {
//          final var tenant = Objects.requireNonNullElse(routingContext.queryParam("tenant").stream().findFirst().orElseThrow(), "default");
//          final var query = new ConfigurationQuery(
//            null,
//            QueryOptionsBuilder.builder()
//              .tenantId(tenant)
//              .build()
//          );
//          configuration.query(query)
//            .flatMap(cfgs -> Multi.createFrom().iterable(cfgs)
//              .onItem().transformToUniAndMerge(cfg -> vertx.fileSystem()
//                .createTempFile(cfg.tClass() + "_" + tenant, ".json")
//                .call(filePath -> vertx.fileSystem().writeFile(filePath, Buffer.newInstance(JsonObject.mapFrom(cfg).toBuffer())))
//              )
//              .collect().asList()
//            )
//            .flatMap(
//              filePaths -> vertx.fileSystem().createTempFile("configuration-", tenant, ".tar.gz")
//                .flatMap(tarBallPath -> vertx.executeBlocking(
//                  Uni.createFrom().item(() -> TarGzipHandler.compress(filePaths, tarBallPath)))
//                )
//            )
//            .subscribe()
//            .with(tarBallPath -> routingContext.response().sendFileAndForget(tarBallPath), routingContext::fail);
//        }
//      );
//    router.post("/configuration/upload/:tenant")
//      .handler(routingContext -> {
//          final var tenant = routingContext.pathParam("tenant");
//          configuration.repositoryHandler().vertx().fileSystem().createTempDirectory("temp-folder")
//            .flatMap(dirPath -> Multi.createFrom().iterable(routingContext.fileUploads())
//              .onItem().transformToUniAndMerge(fileUpload -> decompressTarBall(dirPath, fileUpload)
//                .flatMap(path -> configuration.repositoryHandler().vertx().fileSystem().readDir(dirPath))
//                .onItem().transformToMulti(files -> Multi.createFrom().iterable(files))
//                .onItem().transformToUniAndMerge(file -> configuration.repositoryHandler().vertx().fileSystem().readFile(file)
//                  .flatMap(fileBuffer -> {
//                      final var json = new JsonObject(fileBuffer.getDelegate());
//                      final var fileName = json.getInteger("revision");
//                      final var active = json.getBoolean("active");
//                      final var tClass = json.getString("tClass");
//                      final var data = json.getJsonObject("data");
//                      return configuration.insert(
//                        new ConfigurationRecord(
//                          null,
//                          fileName,
//                          tClass,
//                          data,
//                          active,
//                          BaseRecord.newRecord(tenant)
//                        )
//                      );
//                    }
//                  )
//                )
//                .collect().asList()
//                .replaceWithVoid()
//              )
//              .collect().asList()
//            )
//            .subscribe()
//            .with(avoid -> noContent(routingContext), routingContext::fail);
//        }
//      );
//    router.get("/configuration/:class/:tenant/:revision")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var tenant = routingContext.pathParam("tenant");
//          final var version = Integer.parseInt(Objects.requireNonNullElse(routingContext.pathParam("revision"), "0"));
//          configuration.selectByKey(new ConfigurationKey(tClass, version, tenant))
//            .subscribe()
//            .with(cfg -> ok(routingContext, cfg.data()), routingContext::fail);
//        }
//      );
//    router.get("/configuration/query")
//      .handler(routingContext -> {
//          final var queryOptions = getQueryOptions(routingContext);
//          final var query = new ConfigurationQuery(
//            routingContext.queryParam("configuration"),
//            QueryOptionsBuilder.builder()
//              .tenantId(routingContext.queryParam("tenant").stream().findFirst().orElseThrow())
//              .creationDateFrom(queryOptions.creationDateFrom())
//              .creationDateTo(queryOptions.creationDateTo())
//              .desc(queryOptions.desc())
//              .lastUpdateFrom(queryOptions.lastUpdateFrom())
//              .lastUpdateTo(queryOptions.lastUpdateTo())
//              .build()
//          );
//          configuration.query(query)
//            .subscribe()
//            .with(cfgs -> {
//                final var configurations = cfgs.stream().map(JsonObject::mapFrom).toList();
//                okWithArrayBody(routingContext, new JsonArray(configurations));
//              },
//              routingContext::fail
//            );
//        }
//      );
//    router.delete("/configuration/:class/:tenant/:revision")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var tenant = routingContext.pathParam("tenant");
//          final var version = Integer.parseInt(Objects.requireNonNullElse(routingContext.pathParam("revision"), "0"));
//          configuration.deleteByKey(new ConfigurationKey(tClass, version, tenant))
//            .subscribe()
//            .with(cfg -> noContent(routingContext), routingContext::fail);
//        }
//      );
//    router.put("/configuration/:class/:active/:tenant/:revision")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var active = Boolean.parseBoolean(Objects.requireNonNullElse(routingContext.pathParam("active"), "true"));
//          final var tenant = routingContext.pathParam("tenant");
//          final var version = Integer.parseInt(Objects.requireNonNullElse(routingContext.pathParam("revision"), "0"));
//          final var cfgData = routingContext.body().asJsonObject();
//          configuration.updateByKey(
//              new ConfigurationRecord(
//                null,
//                version,
//                tClass,
//                cfgData,
//                active,
//                BaseRecord.newRecord(tenant)
//              )
//            )
//            .subscribe()
//            .with(cfg -> ok(routingContext, cfg.data()), routingContext::fail);
//        }
//      );
//    router.post("/configuration/:class/:revision/:tenant/:active")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var version = Integer.valueOf(routingContext.pathParam("revision"));
//          final var tenant = routingContext.pathParam("tenant");
//          final var active = Objects.requireNonNullElse(Boolean.valueOf(routingContext.pathParam("active")), Boolean.TRUE);
//          final var cfgData = routingContext.body().asJsonObject();
//          configuration.insert(new ConfigurationRecord(
//                null,
//                version,
//                tClass,
//                cfgData,
//                active,
//                BaseRecord.newRecord(tenant)
//              )
//            )
//            .subscribe()
//            .with(cfg -> ok(routingContext, cfg.data()), routingContext::fail);
//        }
//      );
//  }
//
//  private Uni<String> decompressTarBall(final String dirPath, final FileUpload fileUpload) {
//    return configuration.repositoryHandler().vertx().executeBlocking(Uni.createFrom().item(() -> TarGzipHandler.decompress(fileUpload.fileName(), dirPath)));
//  }
//
//
//}
