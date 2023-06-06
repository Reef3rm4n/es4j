//package io.vertx.eventx.config;
//
//import io.smallrye.mutiny.Multi;
//import io.smallrye.mutiny.Uni;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import io.vertx.eventx.config.orm.ConfigurationKey;
//import io.vertx.eventx.config.orm.ConfigurationQuery;
//import io.vertx.eventx.config.orm.ConfigurationRecord;
//import io.vertx.eventx.http.HttpRoute;
//import io.vertx.eventx.sql.Repository;
//import io.vertx.eventx.sql.models.BaseRecord;
//import io.vertx.mutiny.core.buffer.Buffer;
//import io.vertx.mutiny.ext.web.FileUpload;
//import io.vertx.mutiny.ext.web.Router;
//
//public class ConfigurationRoute implements HttpRoute {
//
//  private final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> configuration;
//  @Override
//  public void registerRoutes(Router router) {
//    router.get("/configuration/download")
//      .handler(routingContext -> {
//          final var metadata = extractMetadata(routingContext);
//          configuration.selectByTenant(metadata.tenant())
//            .onItem().transformToUniAndMerge(cfg -> vertx.fileSystem()
//              .createTempFile(cfg.fileName() + "_" + metadata.tenant().brandId() + "_" + metadata.tenant().partnerId(), ".json")
//              .call(filePath -> vertx.fileSystem().writeFile(filePath, Buffer.newInstance(JsonObject.mapFrom(cfg).toBuffer())))
//            )
//            .collect().asList()
//            .flatMap(
//              filePaths -> vertx.fileSystem().createTempFile("configuration-" + metadata.tenant().brandId() + "_" + metadata.tenant().partnerId(), ".tar.gz")
//                .flatMap(tarBallPath -> vertx.executeBlocking(
//                  Uni.createFrom().item(() -> TarGzipHandler.compress(filePaths, tarBallPath)))
//                )
//            )
//            .subscribe()
//            .with(tarBallPath -> routingContext.response().sendFileAndForget(tarBallPath), routingContext::fail);
//        }
//      );
//    router.post("/configuration/upload")
//      .handler(routingContext -> {
//          final var requestMetadata = extractMetadata(routingContext);
//          vertx.fileSystem().createTempDirectory("temp-folder")
//            .flatMap(dirPath -> Multi.createFrom().iterable(routingContext.fileUploads())
//              .onItem().transformToUniAndMerge(fileUpload -> decompressTarBall(dirPath, fileUpload)
//                .flatMap(path -> vertx.fileSystem().readDir(dirPath))
//                .onItem().transformToMulti(files -> Multi.createFrom().iterable(files))
//                .onItem().transformToUniAndMerge(file -> vertx.fileSystem().readFile(file)
//                  .flatMap(fileBuffer -> {
//                      final var json = new JsonObject(fileBuffer.getDelegate());
//                      final var fileName = json.getString("fileName");
//                      final var tClass = json.getString("tClass");
//                      final var data = json.getJsonObject("data");
//                      return configuration.insert(new ConfigurationRecord(fileName, tClass, data, BaseRecord.newRecord(requestMetadata.tenant())));
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
//    router.get("/configuration/:class/:fileName")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var fileName = routingContext.pathParam("fileName");
//          final var metadata = extractMetadata(routingContext);
//          configuration.selectByKey(new ConfigurationKey(fileName, tClass, metadata.tenant()))
//            .subscribe()
//            .with(cfg -> ok(routingContext, cfg.data()), routingContext::fail);
//        }
//      );
//    router.get("/configuration/query")
//      .handler(routingContext -> {
//          final var metadata = extractMetadata(routingContext);
//          final var query = new ConfigurationQuery(
//            routingContext.queryParam("fileName"),
//            routingContext.queryParam("class"),
//            QueryOptions.from(null, metadata, getQueryOptions(routingContext))
//          );
//          configuration.query(query)
//            .subscribe()
//            .with(cfgs -> {
//                final var configurations = cfgs.stream().map(cfg -> new JsonObject()
//                  .put("fileName", cfg.fileName())
//                  .put("tClass", cfg.tClass())
//                  .put("data", cfg.data())
//                  .put("lastUpdate", cfg.persistedRecord().lastUpdate())
//                  .put("creationDate", cfg.persistedRecord().creationDate())
//                ).toList();
//                okWithArrayBody(routingContext, new JsonArray(configurations));
//              },
//              routingContext::fail
//            );
//        }
//      );
//    router.delete("/configuration/:class/:fileName")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var fileName = routingContext.pathParam("fileName");
//          final var metadata = extractMetadata(routingContext);
//          configuration.deleteByKey(new ConfigurationKey(fileName, tClass, metadata.tenant()))
//            .subscribe()
//            .with(cfg -> noContent(routingContext), routingContext::fail);
//        }
//      );
//    router.put("/configuration/:class/:fileName")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var fileName = routingContext.pathParam("fileName");
//          final var metadata = extractMetadata(routingContext);
//          final var cfgData = routingContext.body().asJsonObject();
//          configuration.updateByKey(new ConfigurationRecord(fileName, tClass, cfgData, BaseRecord.newRecord(metadata.tenant())))
//            .subscribe()
//            .with(cfg -> ok(routingContext, cfg.data()), routingContext::fail);
//        }
//      );
//    router.post("/configuration/:class/:fileName")
//      .handler(routingContext -> {
//          final var tClass = routingContext.pathParam("class");
//          final var fileName = routingContext.pathParam("fileName");
//          final var metadata = extractMetadata(routingContext);
//          final var cfgData = routingContext.body().asJsonObject();
//          configuration.insert(new ConfigurationRecord(fileName, tClass, cfgData, BaseRecord.newRecord(metadata.tenant())))
//            .subscribe()
//            .with(cfg -> ok(routingContext, cfg.data()), routingContext::fail);
//        }
//      );
//  }
//
//  private Uni<String> decompressTarBall(final String dirPath, final FileUpload fileUpload) {
//    return vertx.executeBlocking(Uni.createFrom().item(() -> TarGzipHandler.decompress(fileUpload.fileName(), dirPath)));
//  }
//
//
//}
