package mesosphere.marathon.core.deployment.impl

import akka.Done
import akka.actor.{ ActorRef, Props }
import akka.testkit.TestActor.{ AutoPilot, NoAutoPilot }
import akka.testkit.TestProbe
import mesosphere.marathon.core.deployment.impl.DeploymentActor.Cancel
import mesosphere.marathon.test.MarathonActorSupport
import org.scalatest.{ FunSuiteLike, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Promise }

class StopActorTest extends MarathonActorSupport with FunSuiteLike with Matchers {

  test("Stop") {
    val promise = Promise[Done]()
    val probe = TestProbe()

    probe.setAutoPilot(new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case Cancel(reason) =>
          system.stop(probe.ref)
          NoAutoPilot
      }
    })
    val ref = system.actorOf(Props(classOf[StopActor], probe.ref, promise, new Exception))

    watch(ref)
    expectTerminated(ref)

    Await.result(promise.future, 5.seconds) should be(Done)
  }
}
