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
import io.vertx.sqlclient.Tuple;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

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
    System.out.println("Requesting the data from the last 5 minutes from {} " + routingContext.request().remoteAddress());
    String query = "select * from temperature where tstamp >= now() - INTERVAL '5 minutes'";

    pgPool.preparedQuery(query)
      .execute()
      .onSuccess(rows -> {
        JsonArray data = new JsonArray();
        for (Row row : rows) {
          data.add(new JsonObject()
            .put("uuid", row.getString("uuid"))
            .put("timestamp", row.getValue("tstamp").toString())
            .put("value", row.getValue("value").toString())
          );
        }
        routingContext.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject().put("data", data).encode());
      })
      .onFailure(failure -> {
        System.out.println("Woops " + failure);
        routingContext.fail(500);
      });
  }

  private void getData(RoutingContext routingContext) {
    String query = "select tstamp, value from temperature where uuid = $1";
    String uuid = routingContext.request().getParam("uuid");
    System.out.println("Requesting the data for {} from {} " + uuid + " " + routingContext.request().remoteAddress());

    pgPool.preparedQuery(query)
      .execute(Tuple.of(uuid))
      .onSuccess(rows -> {
        JsonArray data = new JsonArray();
        for (Row row : rows) {
          data.add(new JsonObject()
            .put("temperature", row.getDouble("value"))
            .put("timestamp", row.getTemporal("tstamp").toString())
          );
        }
        routingContext.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put("uuid", uuid)
            .put("data", data).encode());
        })
        .onFailure(failure -> {
          System.out.println("Woops " + failure);
          routingContext.fail(500);
        });
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

  private void recordTemperature(Message<JsonObject> message) {
    JsonObject body = message.body();
    String query = "insert into temperature(uuid, tstamp, value) values ($1, $2, $3);";
    String uuid = body.getString("uuid");
    OffsetDateTime timestamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(body.getLong("timestamp")), ZoneId.systemDefault());
    Double temperature = body.getDouble("temperature");
    Tuple tuple = Tuple.of(uuid, timestamp, temperature);
    pgPool.preparedQuery(query)
      .execute(tuple)
      .onSuccess(rows -> System.out.println(this.getClass().getName() + " recordTemperature:: Recorded {}" + tuple.deepToString()))
      .onFailure(failure -> {
        System.out.println("Woops " + failure);
      });
  }

  public static void main(String[] args) {
    // Configuration code (omitted)
    ClusterManager mgr = new HazelcastClusterManager();
    VertxOptions options = new VertxOptions().setClusterManager(mgr);

    Vertx.clusteredVertx(options)
      .onSuccess(vertx -> {
        vertx.deployVerticle(new PostgresApiVerticle());
      })
      .onFailure(failure -> {
        System.out.println("Woops " + failure);
      });
  }
}
