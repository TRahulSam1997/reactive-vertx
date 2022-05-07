package com.reactive_vertx.reactive_vertx;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
//import io.vertx.core.impl.logging.LoggerFactory;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class Main {
//  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    System.out.println("Starting app... ");

//    Vertx vertx = Vertx.vertx();
//    vertx.deployVerticle(new SensorVerticle(), new DeploymentOptions().setInstances(1));
//    vertx.deployVerticle("SensorVerticle", new DeploymentOptions().setInstances(1));

//    vertx.eventBus()
//      .<JsonObject>consumer("temperature.updates", jsonObjectMessage -> {
//        System.out.println(">>> {}" + jsonObjectMessage.body().encodePrettily());
//      });
    // Configuration code (omitted)
    ClusterManager mgr = new HazelcastClusterManager();
    VertxOptions options = new VertxOptions().setClusterManager(mgr);

    Vertx.clusteredVertx(options)
      .onSuccess(vertx -> {
        vertx.deployVerticle(new SensorVerticle());
      })
      .onFailure(failure -> {
        System.out.println("Woops " + failure);
      });
  }

}
