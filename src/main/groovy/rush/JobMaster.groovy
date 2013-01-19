/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.Terminated
import akka.actor.UntypedActor
import akka.cluster.Cluster
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.LeaderChanged
import akka.cluster.ClusterEvent.MemberDowned
import akka.cluster.ClusterEvent.MemberExited
import akka.cluster.ClusterEvent.MemberJoined
import akka.cluster.ClusterEvent.MemberLeft
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.MemberEvent

import groovy.util.logging.Slf4j
import rush.data.DataStore
import rush.data.NodeData
import rush.data.NodeStatus
import rush.data.WorkerData
import rush.data.WorkerRef
import rush.messages.*
/**
 *  Based on
 *  http://letitcrash.com/post/29044669086/balancing-workload-across-nodes-with-akka-2
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class JobMaster extends UntypedActor  {

    static final ACTOR_NAME = "Master"

    // Holds known workers and what they may be working on
    protected NodeData node

    protected DataStore store

    Cluster cluster

    Address leaderAddress

    Address selfAddress

    Random random = new Random()

    Map<Address,ActorRef> members = new HashMap<>()



    /*
     * When a new entry is added to the jobs storage map, the listener is invoked which will add
     * the job id to the jobs local queue
     */
    private onNewJobAddedListener = { JobEntry job ->
        log.debug "Add new Job event received for: $job"
        if( job.status != JobStatus.NEW ) {
            log.debug "Skipping add for job ${job.id} status is ${job.status} != NEW"
            return
        }

        def message = WorkToSpool.of(job.id)
        log.debug "-> $message"
        getSelf().tell(message)
    }

    def JobMaster( DataStore store ) {
        assert store
        this.store = store
    }


    @Override
    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"
        cluster = Cluster.get(getContext().system())
        selfAddress = cluster.selfAddress()

        // -- load the data structure for this node - or - create a new one
        node = store.getNodeData(selfAddress)
        if( !node ) {
            node = new NodeData()
            //node.name = InetAddress.getLocalHost()?.getHostName()
            node.status = NodeStatus.AVAIL
            node.address = cluster.selfAddress()
            node.startTimestamp = getContext().system().startTime()
            store.putNodeData(node)
        }

        // -- listen for member events
        cluster.subscribe(getSelf(), ClusterEvent.MemberEvent);
        cluster.subscribe(getSelf(), ClusterEvent.LeaderChanged)

        // -- add itself to members map
        members.put(selfAddress, getSelf())

        // --- add the event listener
        store.addNewJobListener( onNewJobAddedListener )
    }

    @Override
    def void postStop() {
        log.debug "~~ Stopping actor ${getSelf().path()}"
        store.putNodeData(node)

        // --- remove the event listener
        store.removeNewJobListener( onNewJobAddedListener )
    }


    def void updateNodeInfo(Closure closure) {
        node = store.updateNodeData(node,closure)
    }


    def void notifyWorkers () {

        if( node.queue.isEmpty() ) {
            log.debug "Empty queue -- nothing to notify"
            return
        }

        if ( node.status == NodeStatus.PAUSED ) {
            log.debug "Node pause -- worker wont be notified"
            return
        }

        // -- notify the workers if required
        node.eachWorker { WorkerData entry ->

            if( !entry.currentJobId ) {
                log.debug "-> WorkIsReady() to ${entry.worker}"
                entry.worker.tell( WorkIsReady.getInstance() )
            }
        }
    }


    @Override
    def void onReceive( def message ) {


        if( message instanceof MemberEvent ) {
            handleMemberEvent(message)
        }

        else {
            handleMessage(message)
            store.putNodeData(node)
        }

    }


    def void handleMessage( def message ) {

        /*
         * -- A client has post a new command to be processed
         *    It is appended to the list of jobs
         */
        if( message instanceof WorkToSpool ) {
            log.debug "<- ${message}"
            final jobId = message.JobId

            // -- verify than an entry exists in the db
            final entry = store.getJob(jobId)
            if( !entry ) {
                // TODO improve this error condition handling
                log.warn "Cannot find any entry for id ${jobId} -- message discarded"
                return
            }

            // -- forward to somebody else
            if( entry.worker?.address() == selfAddress && members.size()>1 ) {
                def target = someoneElse()
                log.debug "=> fwd: ${message} TO someoneElse: ${someoneElse()}"
                target.forward(message,getContext())
            }
            // -- add the jobId to the queue - and - notify workers
            else {
                log.debug("Adding ${jobId} to queue")
                node.queue.add(jobId)
                // update the job status
                entry.status = JobStatus.PENDING
                store.saveJob(entry)
                // notify workers
                notifyWorkers()
            }

        }


        /*
         * Worker is alive. Add him to the list, watch him for
         * death, and let him know if there's work to be done
         */
        else if( message instanceof WorkerCreated ) {
            log.debug "<- ${message}"
            def worker = message.worker
            context.watch(worker.actor)

            node.putWorkerData( WorkerData.of(worker) )
        }

        /*
         * A worker wants more work.  If we know about him, he's not
         * currently doing anything, and we've got something to do,
         * give it to him.
         */
        else if( message instanceof WorkerRequestsWork ) {
            log.debug "<- ${message}"
            final worker = message.worker

            if( node.status == NodeStatus.PAUSED ) {
                log.debug "Node paused -- Worker request for work ignored"
                return
            }

            // dequeue a new jobId to be processed
            // assign to the worker a new job to be processed
            final jobId = node.queue.poll()
            if ( !jobId ){

                // Worker is local?
                // YES -> ask work to other node
                // NO -> no work to done
                if( worker.isLocal(selfAddress) ) {
                    someoneWithWork() ?. forward(message, getContext())
                }
                //log.debug "-> NoWorkToBeDone()"
                //worker.tell( NoWorkToBeDone.getInstance() )
                return
            }

            // Worker is local ?
            // YES -> update the worker map and and notify the worker
            // NO -> send the work to the other node
            if( !worker.isLocal(selfAddress) ) {
                ActorRef actor = members.get( worker.address() ) ?: someoneElse()
                actor.tell( WorkToSpool.of(jobId) )
                return
            }


            // TODO ++++ improve the error condition handling
            try {
                if( !node.hasWorkerData(worker) ) {
                    throw new IllegalStateException("Unknown worker: ${worker} for node ${selfAddress}")
                }

                if( !node.assignJobId(worker, jobId) ) {
                    throw new IllegalStateException("Oops! Worker ${worker} request a new job but is still processing a job")
                }

                // so far everything is ok -- notify the worker
                def request = new WorkToBeDone(jobId)
                log.debug "-> ${request}"
                worker.tell( request )
            }
            catch( Exception failure ) {

                if( failure instanceof IllegalStateException ) {
                    log.warn( failure.getMessage() ?: failure.toString() )
                }
                else {
                    log.error("Unable to process message: $message", failure)
                }

                // re-queue the jobId that was unable to manage
                someoneElse().tell( WorkToSpool.of(jobId) )
            }

        }

        /*
         * Worker has completed its work and we can clear it out
         */
        else if( message instanceof WorkIsDone ) {
            log.debug "<- ${message}"

            final worker = message.worker
            final job = message.job

            if( !node.removeJobId(worker) ) {
                log.error("Blurgh! ${message.worker} said it's done work but we didn't know about him")
            }

            // -- notify the sender the result
            if( job.retryIsRequired() ) {
                def master = someoneElse()
                log.debug "Job ${job.id} failed -- retry submitting to ${master}"
                master.tell( WorkToSpool.of(job.id) )
            }
            else if( job.sender ) {
                log.debug "Reply job result to sender -- ${job.result}"
                job.sender.tell ( job.result, worker )
            }

        }

        /*
         * A worker died. If he was doing anything then we need
         * to give it to someone else so we just add it back to the
         * master and let things progress as usual
         */
        else if( message instanceof Terminated ) {
            log.debug "<- Terminated actor: ${message.getActor}"

            def worker = new WorkerRef(message.actor)

            def jobId =  node.getWorkerData(worker) ?. currentJobId
            if( jobId ) {
                log.error("Blurgh! ${worker} died while processing: ${jobId}")
                // Send the work that it was doing back to ourselves for processing
                someoneElse().tell(WorkToSpool.of(jobId))
            }
            log.debug "Removing worker: ${worker} from availables workers"
            node.removeWorkerData(worker)

        }

        /*
         * A worker send a 'WorkerFailure' message to notify an error condition
         */
        else if ( message instanceof WorkerFailure ) {
            log.debug "<- ${message}"
            final worker = message.worker
            node.failureInc(worker)
        }


        /*
         * pause the node and stop all workers
         */
        else if ( message instanceof PauseWorker ) {
            log.debug "Pausing node: $selfAddress"

            // set the node in 'PAUSED' status
            node.status = NodeStatus.PAUSED

            // -- stop as well all running jobs
            if ( message.hard ) {
                node.workers.keySet().each { WorkerRef ref ->  ref.tell(message) }
            }
        }

        else if( message instanceof  ResumeWorker ) {
            log.debug "Resuming node: $selfAddress"
            node.status = NodeStatus.AVAIL
            notifyWorkers()
        }


        // -- anything else send to dead letters
        else {
            log.debug "Unhandled message: $message"
            unhandled(message)
        }

    }


    /*
     * keep track of the cluster members status
     */
    protected void handleMemberEvent( def message ) {

        // -- leader handling messages

        if (message instanceof CurrentClusterState) {
            def state = (CurrentClusterState) message;
            leaderAddress = state.getLeader()
            log.debug "<- ${message} - leaderAddres: ${leaderAddress} "
        }

        else if (message instanceof LeaderChanged) {
            def leaderChanged = (LeaderChanged) message;
            leaderAddress = leaderChanged.getLeader()
            log.debug "<- ${message} - leaderAddres: ${leaderAddress} "
        }


        // member - IN - message

        else if ( message instanceof MemberJoined ) {
            log.debug "<- ${message}"
            putAddress( message.member().address() )
        }

        else if( message instanceof MemberUp  ) {
            log.debug "<- ${message}"
            putAddress( message.member().address() )
        }


        // member 'out' messages

        else if ( message instanceof MemberExited ) {
            log.debug "<- ${message}"
            removeAddress( message.member().address() )
            resumeJobs( message.member().address() )
        }

        else if(message instanceof MemberDowned  ) {
            log.debug "<- ${message}"
            removeAddress( message.member().address() )
            resumeJobs( message.member().address() )
        }

        else if ( message instanceof MemberRemoved ) {
            log.debug "<- ${message}"
            removeAddress( message.member().address() )
            resumeJobs( message.member().address() )
        }

        else if( message instanceof MemberLeft ) {
            log.debug "<- ${message}"
            removeAddress( message.member().address() )
            resumeJobs( message.member().address() )
        }

        else {
            log.debug "Unused cluster member message: $message"
        }


    }

    protected boolean putAddress( Address address ) {
        log.debug "Putting address: ${address} in the members map"
        def actor = getContext().system().actorFor("${address}/user/${JobMaster.ACTOR_NAME}")
        members.put( address, actor)
        return true
    }

    protected boolean removeAddress( Address address ) {
        log.debug "Removing address ${address} from members map"
        members.remove(address)
        return true
    }

    /**
     *
     * @return
     */
    protected ActorRef getAny() {
        assert members, 'Members map must contain at itself as member'

        // make a copy - and - remove the current address
        List<Address> addresses = new ArrayList(members.keySet())

        // find out a random actor in this list
        def randomAddress = addresses.get(random.nextInt(addresses.size()))
        members.get( randomAddress )

    }

    /**
     *
     * @return
     */
    protected ActorRef someoneElse( ) {
        assert members, 'Members map must contain at itself as member'

        // make a copy - and - remove the current address
        List<Address> addresses = new ArrayList(members.keySet())
        // in order to give priority to other nods
        // if there are at least two nodes, remove the its own address
        if( members.size()>1 ) {
            addresses.remove(selfAddress)
        }

        // find out a random actor in this list
        def randomAddress = addresses.get(random.nextInt(addresses.size()))
        return members.get( randomAddress )
    }

    /**
     *
     * @return
     */
    protected ActorRef someoneWithWork() {
        assert members, 'Members map must contain at itself as member'

        ActorRef actor
        NodeData node = null
        long max = 0
        store.findAllNodesData().each {
            if( it.queue.size()>max && it.address != selfAddress ) {
                max = it.queue.size()
                node = it
            }
        }

        return node ? members[node.address] : null
    }


    protected void resumeJobs( Address nodeAddress ) {
        assert nodeAddress

        final deadNodeData = store.getNodeData(nodeAddress)
        if( deadNodeData == null ) {
            log.debug "No NodeData for address: ${nodeAddress} -- ignore it"
            return
        }

        def OK = store.removeNodeData(deadNodeData)
        if( OK ) {
            log.info "Cleared NodeData for address: ${nodeAddress}"
        }
        else {
            log.warn "Cannot clear NodeData for address: ${nodeAddress} -- luckily someone else has been quickier"
            return
        }

        def jobs = extractLostJobs(deadNodeData)

        jobs.each { JobEntry job  ->
            // -- notify the sender the result
            if( job.retryIsRequired() ) {
                log.debug "-> AddWorkToQueue(${job.id}) "
                getAny().tell( WorkToSpool.of(job.id) )
            }
            else if( job.sender ) {
                log.debug "-> ${job.result} to ${job.sender}"
                job.sender.tell ( job.result )
            }
        }

        deadNodeData.queue.each {
            getAny().tell( WorkToSpool.of( it ) )
        }


    }

    protected List<JobEntry> extractLostJobs( NodeData aNodeData )  {

        def result = []
        aNodeData.eachWorker { WorkerData it ->
            if( it.currentJobId ) {
                def entry = store.getJob(it.currentJobId)
                if( entry ) { result << entry }
                else {
                    log.warn "Unable to retried JobEntry for ${it.currentJobId} "
                }
            }
        }

        return result
    }




}


