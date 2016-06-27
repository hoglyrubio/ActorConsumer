package com.hogly.kafka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class App {

  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("actor-consumer");
    ActorRef actorConsumer = system.actorOf(ActorConsumer.props(), "actorConsumer");
    actorConsumer.tell(ActorConsumer.START_READ, ActorRef.noSender());
  }

}
