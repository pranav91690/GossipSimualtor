import akka.actor.{Props, ActorSystem, ActorRef, Actor}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Random

/**
 * Created by pranav on 9/22/15.
 */
object project2 {
  // Message to Distribute the Message
  case class smartGossip(s:Float, w:Float)
  // Message to Distribute the Message
  case object dumbGossip
  // Tick Object
  case object Tick
  // Message to Notify that the node has heard the rumour Enough Times
  case object heardEnough

  var neighbours:Array[ActorRef] = null
  var numOfNodes:Int = 0
  var cubeArea:Int = 0
  var cubeSide:Int = 0
  var topology:String = null
  var terminatedActors:Int = 0
  var b:Long = 0


  // Listener Actor to Determine if all actors terminated
  // The Listener Actor
  class Listener extends Actor {
    def receive = {
      case `heardEnough` =>
        terminatedActors += 1

        if(terminatedActors == numOfNodes) {
          println("Total Time : " + (System.currentTimeMillis() - b) + " millisecs")
          for(i <- neighbours.indices){
            context.system.stop(neighbours(i))
          }

          // Sleep for Some time
          Thread.sleep(100)
          // ShutDown the System
          println("Shutting Down the System")
          context.system.shutdown()
        }
    }
  }

  // The Dumb Gossip Monger Actor
  class DumbGossipMonger(index : Int, listener: ActorRef) extends Actor{
    // Number of Rumours Received
    var rumourReceived = 0
    // Node Index
    val NodeIndex = index

    def receive = {
      case `dumbGossip` =>
        rumourReceived += 1 // Increase the Count

        // Get the Next Actor
        returnActorIndex(NodeIndex) ! dumbGossip

        // TODO
        // Start a Scheduler the First Time the Node receives a Message
//        import context.dispatcher
//        if(rumourReceived == 1) {
//          context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, Tick)
//        }

        if (rumourReceived == 10) {
          // Terminate this Actor
          listener ! heardEnough
        }

        // TODO
//      case `Tick` =>
//        // Get the index of the Next Actor to send the rumour to and send the message
//        val nextActor = returnActorIndex(NodeIndex)
//        neighbours(nextActor) ! dumbGossip
    }
  }

  // The Smart Gossip Monger Actor
  class SmartGossipMonger(index:Int, listener: ActorRef) extends Actor {
    var s:Float = 0
    var w:Float = 0
    var sumEstimate:Float = 0
    var streak = 0

    // Node Index
    val NodeIndex = index

    def receive = {
      case smartGossip(sR, wR) =>

        // Keep half of the received value and send the other half
        val oldSumEstimate = sumEstimate
        s = s + sR / 2
        w = w + wR / 2
        sumEstimate = s / w
        val difference = math.abs(sumEstimate - oldSumEstimate)
        if (difference <= math.pow(10, -1)) {
          streak += 1
        } else {
          streak = 0
        }

        // Send to Next Actor
        returnActorIndex(NodeIndex) ! smartGossip(sR/2, wR/2)

        // Terminate this Actor, Based on the ratio s/w
        if(streak == 3) {
          listener ! heardEnough
        }

    }
  }

  def returnActorIndex(index : Int) : ActorRef = {
    // Return Index Based on the Topology
    topology match {
      case "full" =>
        // In this Toplogy, select any actor send it the message
        val random = new Random()
        var notSameActor: Boolean = false
        var randomIndex = 0
        while (!notSameActor) {
          randomIndex = random.nextInt(numOfNodes)
          if (randomIndex != index) {
            notSameActor = true
          }
        }

        neighbours(randomIndex)

      case "3D" =>
        // Declare A Mutable ArrayBuffer
        val List = new ArrayBuffer[ActorRef]()
        // Based on the Cube Side, find the Neighbours
        // West Neighbour
        if (index % cubeSide != 0) {
          List.append(neighbours(index - 1))
        }
        // East Neighbour
        if (index % cubeSide != cubeSide - 1) {
          List.append(neighbours(index + 1))
        }
        // South Neighbour
        if (index % cubeArea >= cubeSide) {
          List.append(neighbours(index - cubeSide))
        }
        // North Neighbour
        if (index % cubeArea < (cubeSide - 1) * cubeSide) {
          List.append(neighbours(index + cubeSide))
        }
        // Bottom Neighbour
        if (index >= cubeArea) {
          List.append(neighbours(index - cubeArea))
        }
        // Top Neighbour
        if (index < (cubeSide - 1) * cubeArea) {
          List.append(neighbours(index + cubeArea))
        }

        // Select a Random Actor from list of neighbours and send the Message
        val random = new Random()
        val randomIndex = random.nextInt(List.size)

        List(randomIndex)

      case "line" =>
        //In this Topology, only the left and right elements are neighbours
        if ((index == 0) || (index == numOfNodes - 1)) {
          if (index == 0) {
            neighbours(1)
          } else {
            neighbours(numOfNodes - 2)
          }
        } else {
          val List = new Array[ActorRef](2)
          List(0) = neighbours(index - 1)
          List(1) = neighbours(index + 1)
          val random = new Random()
          val randomIndex = random.nextInt(2)
          List(randomIndex)
        }

      case "imp3D" =>
        // Declare A Mutable ArrayBuffer
        val List = new ArrayBuffer[ActorRef]()
        val n = new ArrayBuffer[Int]()
        // Based on the Cube Side, find the Neighbours
        // West Neighbour
        val west = index - 1
        if (index % cubeSide != 0) {
          List.append(neighbours(west))
          n.append(west)
        }
        // East Neighbour
        val east = index + 1
        if (index % cubeSide != cubeSide - 1) {
          List.append(neighbours(east))
          n.append(east)
        }
        // South Neighbour
        val south = index - cubeSide
        if (index % cubeArea >= cubeSide) {
          List.append(neighbours(south))
          n.append(south)
        }
        // North Neighbour
        val north = index + cubeSide
        if (index % cubeArea < (cubeSide - 1) * cubeSide) {
          List.append(neighbours(north))
          n.append(north)
        }
        // Bottom Neighbour
        val bottom = index - cubeArea
        if (index >= cubeArea) {
          List.append(neighbours(bottom))
          n.append(bottom)
        }
        // Top Neighbour
        val top = index + cubeArea
        if (index < (cubeSide - 1) * cubeArea) {
          List.append(neighbours(top))
          n.append(top)
        }

        n.append(index)

        // Random Neighbour
        val random2 = new Random()
        var notSameActor: Boolean = false
        var randomIndex2: Int = 0
        while (!notSameActor) {
          randomIndex2 = random2.nextInt(numOfNodes)
          // If this index is not already seen
          if (!n.contains(randomIndex2)) {
            notSameActor = true
          }
        }
        List.append(neighbours(randomIndex2))

        // Select a Random Actor from list of neighbours and send the Message
        val random = new Random()
        val randomIndex = random.nextInt(List.size)
        List(randomIndex)
    }
  }
  // Main Method
  def main (args: Array[String]) {
      // Get the Number of Nodes
      numOfNodes = args(0).toInt
      topology = args(1)

      topology match  {
        case "full" =>
          neighbours = new Array[ActorRef](numOfNodes)

        case "3D" =>
          // Change the Number of Nodes Accordingly
          cubeSide = math.ceil(math.cbrt(numOfNodes)).toInt
          cubeArea = math.pow(cubeSide,2).toInt
          numOfNodes = math.pow(cubeSide,3).toInt
          neighbours = new Array[ActorRef](numOfNodes)

        case "line" =>
          neighbours = new Array[ActorRef](numOfNodes)

        case "imp3D" =>
          cubeSide = math.ceil(math.cbrt(numOfNodes)).toInt
          cubeArea = math.pow(cubeSide,2).toInt
          numOfNodes = math.pow(cubeSide,3).toInt
          neighbours = new Array[ActorRef](numOfNodes)
      }

      // Get the Algorithm and run it on the above mentioned topology
      args(2) match  {
        case "gossip" => runGossip()

        case "push-sum" => runPushSum()
      }
  }

  // Gossip Algo Logic
  def runGossip()  = {
    // Start the Actor System and Instantiate the Actors
    val system = ActorSystem("GossipManager")

    // Create the Listener Actor
    val listener = system.actorOf(Props[Listener], name = "listener")

    for(i <- 0 until numOfNodes){
      // Create the Actor
      neighbours(i) = system.actorOf(Props(new DumbGossipMonger(i, listener)))
    }

    // Randomly Select one Actor and Start the Gossip
    val random = new Random()
    val randomIndex = random.nextInt(numOfNodes)
    // Using the Random Index, send the Start Gossip Message to that Actor
    println("Started Gossip")
    b = System.currentTimeMillis()
    neighbours(randomIndex) ! dumbGossip
  }

  // Push Sum Logic
  def runPushSum()  = {
    // Start the Actor System and Instantiate the Actors
    val system = ActorSystem("GossipManager")

    // Create the Listener Actor
    val listener = system.actorOf(Props[Listener], name = "listener")

    for(i <- 0 until numOfNodes){
      neighbours(i) = system.actorOf(Props(new SmartGossipMonger(i, listener)))
    }

    // Randomly Select one Actor and Start the Gossip
    val random = new Random()
    val randomIndex = random.nextInt(numOfNodes)
    // Using the Random Index, send the Start Gossip Message to that Actor
    println("Started PushSum")
    b = System.currentTimeMillis()
    neighbours(randomIndex) ! smartGossip(randomIndex + 1, 1)
  }
}
