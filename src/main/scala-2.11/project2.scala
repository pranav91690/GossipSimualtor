import akka.actor._
import scala.collection.mutable
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
  case object converged
  // Gossip
  case object heardEnoughDumb
  // Message to Indicate the node heard the rumour once
  case object heardOnce

  var Nodes:Array[ActorRef] = null
  var numOfNodes:Int = 0
  var cubeArea:Int = 0
  var cubeSide:Int = 0
  var topology:String = null
  var b:Long = 0

  var nodeNeighbours : mutable.HashMap[Int, ArrayBuffer[ActorRef]] = null

  // Listener Actor to Determine if all actors terminated
  // The Listener Actor
  class Listener extends Actor {
    var satisfiedNodes = 0
    def receive = {
      case `converged` =>
        satisfiedNodes += 1
        if (satisfiedNodes == numOfNodes) {
          println("Total Time : " + (System.currentTimeMillis() - b) + " millisecs")
          println("Shutting Down the System")
          context.system.shutdown()
        }

      case `heardOnce` =>
        satisfiedNodes += 1
        if(satisfiedNodes == numOfNodes){
          println("Total Time : " + (System.currentTimeMillis() - b) + " millisecs")
          println("Shutting Down the System")
          context.system.shutdown()
        }
    }
  }

  def returnNextNode(index : Int) : ActorRef = {
    topology match {
      case "full" =>
        // Get a Next Node, Check if it's same node
        var next = returnRandomNode()
        while (next == index){
          next = returnRandomNode()
        }
        Nodes(next)
      case "line" =>
        val random = new Random()
        val List = nodeNeighbours(index)
        List(random.nextInt(List.size))
      case "3D" =>
        val random = new Random()
        val List = nodeNeighbours(index)
        List(random.nextInt(List.size))
      case "imp3D" =>
        val random = new Random()
        val List = nodeNeighbours(index)
        var next = returnRandomNode()
        while (next == index){
          next = returnRandomNode()
        }
        List.append(Nodes(next))
        List(random.nextInt(List.size))
    }
  }

  // The Dumb Gossip Monger Actor
  class DumbGossipMonger(index : Int, listener: ActorRef) extends Actor{
    // Number of Rumours Received
    var rumourReceived = 0
    // Node Index
    val NodeIndex = index
    // Flag to notify if the node is receiving the message for the first time
    var firstMessage = true
    // Flag to to stop the Node if it has heard the Rumour "some" times
    var terminate = false
    // Variable to Store the cancellable object from the Scheduler
    var cancel:Cancellable = null

    def receive = {
      case `dumbGossip` =>
        if(firstMessage){
          // Send the Message it Heard Once
          listener ! heardOnce
          import context.dispatcher
          cancel = context.system.scheduler.schedule(0 milliseconds, 5 milliseconds, self, Tick)
          firstMessage = false
        }

        rumourReceived += 1 // Increase the Count

        // Get the Next Actor
        if(!terminate) {
          returnNextNode(NodeIndex) ! dumbGossip
        }

        if (rumourReceived == 10) {
          // Terminate this Actor
          terminate = true
          // Cancel the Schedule
          cancel.cancel()
        }

      case `Tick` =>
        // Get the index of the Next Actor to send the rumour to and send the message
        if(!terminate) {
          returnNextNode(NodeIndex) ! dumbGossip
        }
    }
  }

  // The Smart Gossip Monger Actor
  class SmartGossipMonger(index:Int, listener: ActorRef) extends Actor {
    var s:Float = index + 1
    var w:Float = 1
    var sumEstimate:Float = 0
    var streak = 0
    var firstMessage = true
    val NodeIndex = index

    def receive = {
      case smartGossip(sR, wR) =>
        if(firstMessage){
          import context.dispatcher
          context.system.scheduler.schedule(0 milliseconds, 3 milliseconds, self, Tick)
          firstMessage = false
        }
        // Add the values to the present values , keep the half and send half
        val oldSumEstimate = sumEstimate
        s = s + sR
        w = w + wR
        val sHalf = s/2
        val wHalf = w/2
        s = sHalf
        w = wHalf
        sumEstimate = s / w
        val difference = math.abs(sumEstimate - oldSumEstimate)
        if (difference <= math.pow(10, -10)) {
          streak += 1
        } else {
          streak = 0
        }

        // Send to Next Actor
        returnNextNode(NodeIndex) ! smartGossip(sHalf, wHalf)

        // Terminate this Actor, Based on the ratio s/w
        if(streak == 3) {
          listener ! converged
        }

      case `Tick` =>
        // Get the index of the Next Actor to send the rumour to and send the message
        val sHalf = s/2
        val wHalf = w/2
        s = sHalf
        w = wHalf
        returnNextNode(NodeIndex) ! smartGossip(sHalf, wHalf)
    }
  }

  // Main Method
  def main (args: Array[String]) {
      // Get the Number of Nodes
      numOfNodes = args(0).toInt
      topology = args(1)

      topology match  {
        case "full" =>
          Nodes = new Array[ActorRef](numOfNodes)

        case "3D" =>
          // Change the Number of Nodes Accordingly
          cubeSide = math.ceil(math.cbrt(numOfNodes)).toInt
          cubeArea = math.pow(cubeSide,2).toInt
          numOfNodes = math.pow(cubeSide,3).toInt
          Nodes = new Array[ActorRef](numOfNodes)

        case "line" =>
          Nodes = new Array[ActorRef](numOfNodes)

        case "imp3D" =>
          cubeSide = math.ceil(math.cbrt(numOfNodes)).toInt
          cubeArea = math.pow(cubeSide,2).toInt
          numOfNodes = math.pow(cubeSide,3).toInt
          Nodes = new Array[ActorRef](numOfNodes)
      }

      // Get the Algorithm and run it on the above mentioned topology
      args(2) match  {
        case "gossip" => runGossip()

        case "push-sum" => runPushSum()
      }
  }

  // Build Neighbours
  def buildNeighbours(): Unit = {
    topology match  {
      case "full" =>

      case "3D" =>
        build3D()

      case "line" =>
        //In this Topology, only the left and right elements are neighbours
        for(index <- Nodes.indices) {
          val List = new ArrayBuffer[ActorRef]()
          if ((index == 0) || (index == numOfNodes - 1)) {
            if (index == 0) {
              List.append(Nodes(1))
            } else {
              List.append(Nodes(numOfNodes - 2))
            }
          } else {
            List.append(Nodes(index - 1))
            List.append(Nodes(index + 1))
          }

          nodeNeighbours(index) = List
        }

      case "imp3D" =>
        build3D()
    }
  }

  def build3D(): Unit ={
    // Build the Neighbours List for Each Node
    // Declare A Mutable ArrayBuffer
    for(index <- Nodes.indices) {
      val List = new ArrayBuffer[ActorRef]()
      // Based on the Cube Side, find the Neighbours
      // West Neighbour
      if (index % cubeSide != 0) {
        List.append(Nodes(index - 1))
      }
      // East Neighbour
      if (index % cubeSide != cubeSide - 1) {
        List.append(Nodes(index + 1))
      }
      // South Neighbour
      if (index % cubeArea >= cubeSide) {
        List.append(Nodes(index - cubeSide))
      }
      // North Neighbour
      if (index % cubeArea < (cubeSide - 1) * cubeSide) {
        List.append(Nodes(index + cubeSide))
      }
      // Bottom Neighbour
      if (index >= cubeArea) {
        List.append(Nodes(index - cubeArea))
      }
      // Top Neighbour
      if (index < (cubeSide - 1) * cubeArea) {
        List.append(Nodes(index + cubeArea))
      }

      nodeNeighbours(index) = List
    }
  }


  // Gossip Algo Logic
  def runGossip() = {
    // Start the Actor System and Instantiate the Actors
    val system = ActorSystem("GossipManager")

    // Create the Listener Actor
    val listener = system.actorOf(Props[Listener], name = "listener")

    for(i <- 0 until numOfNodes){
      // Create the Actor
      Nodes(i) = system.actorOf(Props(new DumbGossipMonger(i, listener)))
    }

    nodeNeighbours = new mutable.HashMap[Int, ArrayBuffer[ActorRef]]

    // Build the Node Neighbours
    buildNeighbours()

    // Randomly Select one Actor and Start the Gossip
//    val random = new Random()
//    val randomIndex = random.nextInt(numOfNodes)
    val randomIndex = returnRandomNode()
    // Using the Random Index, send the Start Gossip Message to that Actor
    println("Started Gossip")
    b = System.currentTimeMillis()
    Nodes(randomIndex) ! dumbGossip
  }

  // Push Sum Logic
  def runPushSum()  = {
    // Start the Actor System and Instantiate the Actors
    val system = ActorSystem("GossipManager")

    // Create the Listener Actor
    val listener = system.actorOf(Props[Listener], name = "listener")

    for(i <- 0 until numOfNodes){
      Nodes(i) = system.actorOf(Props(new SmartGossipMonger(i, listener)))
    }

    nodeNeighbours = new mutable.HashMap[Int, ArrayBuffer[ActorRef]]


    // Build the Node Neighbours
    buildNeighbours()

    // Randomly Select one Actor and Start the Gossip
//    val random = new Random()
//    val randomIndex = random.nextInt(numOfNodes)
    val randomIndex = returnRandomNode()
    // Using the Random Index, send the Start Gossip Message to that Actor
    println("Started PushSum")
    b = System.currentTimeMillis()
    Nodes(randomIndex) ! smartGossip(randomIndex + 1, 1)
  }

  def returnRandomNode() : Int = {
    val random = new Random()
    val randomIndex = random.nextInt(numOfNodes)
    randomIndex
  }
}
