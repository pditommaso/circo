package circo.frontend

import akka.actor.ActorRef
import akka.camel.CamelMessage
import akka.camel.javaapi.UntypedConsumerActor
import groovy.util.logging.Slf4j

/**
 * TCP public accessible front-end
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class TcpFacade extends UntypedConsumerActor {

    static final String ACTOR_NAME = 'TcpFacade'

    final String bindAddress

    final ActorRef frontEnd

    final String endPoint

    def TcpFacade( String bindAddress ) {
        assert bindAddress
        frontEnd = getContext().system().actorFor "/user/${FrontEnd.ACTOR_NAME}"
        endPoint = "netty:tcp://${bindAddress}?keepAlive=true"
        log.debug "TcpFacade end-point: '$endPoint'"
    }

    @Override
    String getEndpointUri() {
        endPoint
    }

    @Override
    void onReceive(Object message) {
        log.debug "<- tcp message $message"

        if (message instanceof CamelMessage) {
            message.body()
            frontEnd.forward(message.body(), getContext())

        } else {
            unhandled(message);
        }


    }
}
