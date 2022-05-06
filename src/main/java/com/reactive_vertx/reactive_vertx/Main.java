package com.reactive_vertx.reactive_vertx;

import io.vertx.core.Vertx;
//import io.vertx.core.impl.logging.LoggerFactory;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class Main {
//  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    System.out.println("Starting app... ");
    
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new SensorVerticle());
  }

}
