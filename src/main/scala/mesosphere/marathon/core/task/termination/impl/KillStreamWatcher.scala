package mesosphere.marathon.core.task.termination.impl

import akka.actor.ActorSystem
import akka.Done
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink, Source }
import akka.stream.OverflowStrategy
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.event.{ InstanceChanged, UnknownInstanceTerminated }
import mesosphere.marathon.core.instance.Instance
import scala.concurrent.Future

object KillStreamWatcher extends StrictLogging {

  def eventBusSource[T](eventStream: akka.event.EventStream)(implicit classTag: scala.reflect.ClassTag[T]): Source[T, Unit] = {
    val source = Source.actorRef(32, OverflowStrategy.fail)

    source.
      mapMaterializedValue { ref =>
        eventStream.subscribe(ref, classTag.runtimeClass)
      }
  }

  def watchForKilledInstances(eventStream: akka.event.EventStream, instanceIds: Seq[Instance.Id]): RunnableGraph[Future[Done]] = {
    import mesosphere.marathon.core.task.termination.InstanceChangedPredicates.considerTerminal

    val killedViaInstanceChanged =
      eventBusSource[InstanceChanged](eventStream).collect {
        case event if considerTerminal(event.condition) && instanceIds.contains(event.id) =>
          event.id
      }

    val killedViaUnknownInstanceTerminated =
      eventBusSource[UnknownInstanceTerminated](eventStream).collect {
        case UnknownInstanceTerminated(id, _, _) if instanceIds.contains(id) =>
          id
      }

    killedViaInstanceChanged.
      merge(killedViaUnknownInstanceTerminated).
      statefulMapConcat { () =>
        var pendingInstanceIds = instanceIds.toSet

        { (id: Instance.Id) =>
          pendingInstanceIds -= id
          logger.debug(s"Received terminal update for ${id}")
          if (pendingInstanceIds.isEmpty) {
            logger.info("All instances are killed, done")
            List(Done)
          }  else {
            logger.info("still waiting for instances to be killed: ${pendingInstanceIds.mkString(",")}")
            Nil
          }
        }
      }.toMat(Sink.head)(Keep.right)
  }
}
