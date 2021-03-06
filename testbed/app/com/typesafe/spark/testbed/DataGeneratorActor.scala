package com.typesafe.spark.testbed

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import java.nio.channels.AsynchronousSocketChannel
import akka.actor.PoisonPill
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Cancellable
import scala.collection.mutable
import scala.annotation.tailrec

class DataGeneratorActor(scheduler: ActorRef) extends Actor {

  def receive = initialState

  val initialState: Actor.Receive = {
    case DataGeneratorActor.TestPlanMsg(testPlan) =>
      println("New test plan. DataGenerator waking up.")
      context.become(executeNewTestPlan(testPlan, sender))
  }

  def executeNewTestPlan(testPlan: TestPlan, requestor: ActorRef): Actor.Receive = {
    import scala.concurrent.duration._
    val tickTask = context.system.scheduler.schedule(0.second, 1.second, self, DataGeneratorActor.TickMsg)
    val startTime = System.currentTimeMillis() / 1000 * 1000 + 1000 // beginning of the next second
    val dataGenerator = new DataGenerator(testPlan)

    executeTestPlan(dataGenerator, startTime, 0, requestor, tickTask)
  }

  def executeTestPlan(
    dataGenerator: DataGenerator,
    startTime: Long,
    tick: Int,
    requestor: ActorRef,
    tickTask: Cancellable): Actor.Receive = {
    case DataGeneratorActor.TestPlanMsg(testPlan) =>
      tickTask.cancel()
      scheduler ! EpochSchedulerActor.clearMsg
      context.become(executeNewTestPlan(testPlan, sender))
    case DataGeneratorActor.TickMsg =>
      dataGenerator.valuesFor(tick).foreach { data =>
        scheduler ! EpochSchedulerActor.ScheduleMsg(data.shiftTime(startTime))
      }
      if (dataGenerator.isDoneAt(tick)) {
        // test plan is done, return in waiting state
        // scheduler continues to push the scheduled messages
        println("Test plan done. DataGenerator going to sleep.")
        tickTask.cancel()
        if (requestor != context.system.deadLetters) {
          requestor ! DataGeneratorActor.TestPlanDoneMsg(0)
        }
        context.become(initialState)
      } else {
        // continue test plan
        context.become(executeTestPlan(dataGenerator, startTime, tick + 1, requestor, tickTask))
      }
  }

}

object DataGeneratorActor {

  case object StopMsg
  case object TickMsg
  case class TestPlanMsg(testPlan: TestPlan)
  case class TestPlanDoneMsg(status: Int)

  def props(scheduler: ActorRef) = Props(classOf[DataGeneratorActor], scheduler)

}

class EpochSchedulerActor(serverManager: ActorRef) extends Actor {

  private val scheduledItems = mutable.PriorityQueue[DataAtTime]() /* mutable !! */

  def receive = emptyState

  private val emptyState: Actor.Receive = {
    case EpochSchedulerActor.ScheduleMsg(data) =>
      println("Scheduler waking up.")
      context.become(startScheduler(data))
  }

  private def startScheduler(firstData: DataAtTime): Actor.Receive = {
    import scala.concurrent.duration._
    val tickTask = context.system.scheduler.schedule(0.second, 10.microsecond, self, EpochSchedulerActor.TickMsg)
    scheduledItems += firstData
    runningState(tickTask)
  }

  private def runningState(tickTask: Cancellable): Actor.Receive = {
    case EpochSchedulerActor.ScheduleMsg(data) =>
      scheduledItems += data
    case EpochSchedulerActor.TickMsg =>
      pushReadyItems()
      if (scheduledItems.isEmpty) {
        tickTask.cancel()
        println("Scheduler queue empty. Going to sleep.")
        context.become(emptyState)
      }
    case EpochSchedulerActor.clearMsg =>
      scheduledItems.clear()
  }

  private def pushReadyItems() {
    val currentTime = System.currentTimeMillis()
    @tailrec
    def loop() {
      scheduledItems.headOption match {
        case Some(head) if head.time <= currentTime =>
          serverManager ! ServerManagerActor.SendInts(head.values)
          scheduledItems.dequeue()
          loop()
        case _ =>
      }
    }
    loop()
  }

}

object EpochSchedulerActor {

  case class ScheduleMsg(data: DataAtTime)
  case object clearMsg
  private case object TickMsg

  def props(serverManager: ActorRef) = Props(classOf[EpochSchedulerActor], serverManager)
}

class ServerManagerActor(socketPort: Int) extends Actor {

  def receive = initialization

  val initialization: Actor.Receive = {
    case ServerManagerActor.StartMsg =>
      val server = Server(self, socketPort)
      context.become(processConnectionsAndData(server, Nil))
  }

  def processConnectionsAndData(server: Server, connectionActors: List[ActorRef]): Actor.Receive = {
    case Server.IncomingConnectionMsg(socket) =>
      val connectionActor = context.actorOf(ConnectionManagerActor.props(socket, self))
      context.become(processConnectionsAndData(server, connectionActor :: connectionActors))
    case ServerManagerActor.ConnectionClosedMsg(connectionActor) =>
      context.become(processConnectionsAndData(server, connectionActors.filterNot { _ == connectionActor }))
    case ServerManagerActor.StopMsg =>
      connectionActors.foreach { _ ! ServerManagerActor.StopMsg }
      server.close()
      context.become(initialization)
    case m: ServerManagerActor.SendInts =>
      connectionActors.foreach { _ ! m }
  }

}

object ServerManagerActor {

  case object StartMsg
  case object StopMsg
  case class ConnectionClosedMsg(connectionActor: ActorRef)
  case class SendInts(is: List[Int])

  def props(socketPort: Int) = Props(classOf[ServerManagerActor], socketPort)
}

class ConnectionManagerActor(socket: AsynchronousSocketChannel, serverManager: ActorRef) extends Actor {

  def receive = {
    case Server.ConnectionClosedMsg =>
      serverManager ! ServerManagerActor.ConnectionClosedMsg(self)
      self ! PoisonPill
    case ServerManagerActor.StopMsg =>
      socket.close()
      self ! PoisonPill
    case ServerManagerActor.SendInts(is) =>
      socket.write(ByteBuffer.wrap(is.mkString("", "\n", "\n").getBytes))
  }

}

object ConnectionManagerActor {
  def props(socket: AsynchronousSocketChannel, serverManager: ActorRef) = Props(classOf[ConnectionManagerActor], socket, serverManager)
}
