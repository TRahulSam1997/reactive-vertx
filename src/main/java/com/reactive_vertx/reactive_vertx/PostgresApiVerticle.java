package com.reactive_vertx.reactive_vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlClient;

public class PostgresApiVerticle extends AbstractVerticle {

  private static final int httpPort = Integer.parseInt(System.getenv().getOrDefault(
    "HTTP_PORT", "8080"
  ));

  private PgPool pgPool;

  @Override
  public void start(Promise<Void> startPromise) {
    PgConnectOptions connectOptions = new PgConnectOptions()
      .setPort(5432)
      .setHost("127.0.0.1")
      .setDatabase("postgres")
      .setUser("postgres")
      .setPassword("root");

    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(5);

    vertx.eventBus().consumer("temperature.updates", this::recordTemperature);

//    SqlClient client = PgPool.client(vertx, connectOptions, poolOptions);

    pgPool = PgPool.pool(vertx, connectOptions, poolOptions);

    Router router = Router.router(vertx);
    router.get("/all").handler(this::getAllData);
    router.get("/for/:uuid").handler(this::getData);
    router.get("/last-5-minutes").handler(this::getLastFiveMinutes);

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(httpPort)
      .onSuccess(ok -> {
        System.out.println(this.getClass().getName() + ": http server running: http://127.0.0.1:{} " + httpPort);
        startPromise.complete();
      })
      .onFailure(startPromise::fail);
//    pgPool = PgPool.pool(vertx, new PgConnectOptions())
//      .setPort(5432)
//      .setHost("127.0.0.1")
//      .setDatabase("postgres")
//      .setUser("postgres")
//      .setPassword("root"), new PoolOptions();

  }

  private void getLastFiveMinutes(RoutingContext routingContext) {
  }

  private void getData(RoutingContext routingContext) {
  }

  private void getAllData(RoutingContext routingContext) {
    System.out.println("Requesting all data from {} " + routingContext.request().remoteAddress());
    String query = "select * from temperature";
    pgPool.preparedQuery(query)
      .execute()
      .onSuccess(rows -> {
        JsonArray array = new JsonArray();
        for (Row row : rows) {
          array.add(new JsonObject()
            .put("uuid", row.getString("uuid"))
            .put("temperature", row.getDouble("value"))
            .put("timestamp", row.getTemporal("tstamp").toString())
          );
        }
        routingContext.response()
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject().put("data", array).encode());
      })
      .onFailure(failure -> {
        System.out.println("Woops " + failure);
        routingContext.fail(500);
      });
  }

  private <T> void recordTemperature(Message<T> tMessage) {


  }
}
