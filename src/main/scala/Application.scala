import java.util.concurrent.ThreadLocalRandom

import Customer.{Done, Work}
import Manager.RequestWork
import Worker.{Complete, Download}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.collection.mutable
import scala.io.Source

object Worker {
  def prop = Props(new Worker())

  case object Download
  case object Error
  case object Complete
}

class Worker extends Actor {
  override def receive: Receive = {
    case (Download, url: String, dir: String) =>
      // TODO
      println("Downloding: " + url)
      Thread.sleep(ThreadLocalRandom.current().nextLong(1000))

      sender() ! Complete
  }
}

object Manager {
  def prop(customer: ActorRef, baseUrl: String, downloadPath: String) = Props(new Manager(customer, baseUrl, downloadPath))

  case object RequestWork
}

class Manager(customer: ActorRef, baseUrl: String, downloadPath: String) extends Actor {
  private var workers = mutable.Queue[ActorRef]()

  for (_ <- 0 until 100) {
    workers += context.actorOf(Worker.prop)
  }

  customer ! RequestWork

  override def receive: Receive = {
    case (Customer.Work, fileName) =>
      val worker = workers.dequeue()
      working += worker
      worker ! (Worker.Download, baseUrl + fileName, downloadPath)
    case Customer.Done =>
      context.become(done)
    case Worker.Complete =>
      working -= sender()
      workers += sender()
    case Worker.Error => println()
  }

  def done: Receive = {
    case Worker.Complete =>
      working -= sender()
      workers += sender()
  }

  private var working = Set[ActorRef]()
}

object Customer {
  def prop(fileName: String) = Props(new Customer(fileName))

  case object Work
  case object Done
}

class Customer(fileName: String) extends Actor {
  private val it = Source.fromFile(fileName).getLines()
  if (it.hasNext) {
    it.next()
  }

  override def receive: Receive = {
    case RequestWork =>
      if (it.hasNext) {
        sender() ! (Work, it.next().split(",")(0).replace("\"", ""))
      } else {
        sender() ! Done
      }
  }
}

object Application extends App {
  val system = ActorSystem("download")

  val customer = system.actorOf(Customer.prop("/Users/xxx/Temp/bbc.txt"))
  val manager = system.actorOf(Manager.prop(customer,
    "http://bbcsfx.acropolis.org.uk/assets/",
    "/Users/xxx/Temp/bbc/"))
}
