/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.apache.rocketmq.mqtt.meta.raft.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

@Service
public class MqttRaftServer {
    private static final Logger logger = LoggerFactory.getLogger(MqttRaftServer.class);

    @Resource
    private MetaConf metaConf;

    private static ExecutorService raftExecutor;
    private static ExecutorService requestExecutor;


    private PeerId localPeerId;
    private NodeOptions nodeOptions;
    private Configuration initConf;
    private RpcServer rpcServer;
    private Map<String, List<RaftGroupHolder>> multiRaftGroup = new ConcurrentHashMap<>();
    private Collection<StateProcessor> stateProcessors = Collections.synchronizedSet(new HashSet<>());

    @PostConstruct
    void init() {
        raftExecutor = new ThreadPoolExecutor(
                8,
                16,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryImpl("RaftExecutor_"));
        requestExecutor = new ThreadPoolExecutor(
                8,
                16,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryImpl("requestExecutor_"));


        localPeerId = PeerId.parsePeer(metaConf.getSelfAddress());
        nodeOptions = new NodeOptions();

        nodeOptions.setSharedElectionTimer(true);
        nodeOptions.setSharedVoteTimer(true);
        nodeOptions.setSharedStepDownTimer(true);
        nodeOptions.setSharedSnapshotTimer(true);
        nodeOptions.setElectionTimeoutMs(metaConf.getElectionTimeoutMs());
        nodeOptions.setEnableMetrics(true);
        nodeOptions.setSnapshotIntervalSecs(metaConf.getSnapshotIntervalSecs());

        // Jraft implements parameter configuration internally. If you need to optimize, refer to https://www.sofastack.tech/projects/sofa-jraft/jraft-user-guide/
        RaftOptions raftOptions = new RaftOptions();
        nodeOptions.setRaftOptions(raftOptions);

        initConf = JRaftUtils.getConfiguration(metaConf.getMembersAddress());
        nodeOptions.setInitialConf(initConf);

        for (PeerId peerId:initConf.getPeers()) {
            NodeManager.getInstance().addAddress(peerId.getEndpoint());
        }

        rpcServer = createRpcServer(this, localPeerId);
        rpcServer.init(null);
        if (!this.rpcServer.init(null)) {
            logger.error("Fail to init [BaseRpcServer].");
            throw new RuntimeException("Fail to init [BaseRpcServer].");
        }

        registerStateProcessor(new CounterStateProcessor());
        start();
    }

   public RpcServer createRpcServer(MqttRaftServer server, PeerId peerId) {
       GrpcRaftRpcFactory raftRpcFactory = (GrpcRaftRpcFactory) RpcFactoryHelper.rpcFactory();
       raftRpcFactory.registerProtobufSerializer(WriteRequest.class.getName(), WriteRequest.getDefaultInstance());
       raftRpcFactory.registerProtobufSerializer(ReadRequest.class.getName(), ReadRequest.getDefaultInstance());
       raftRpcFactory.registerProtobufSerializer(Response.class.getName(), Response.getDefaultInstance());

       MarshallerRegistry registry = raftRpcFactory.getMarshallerRegistry();
       registry.registerResponseInstance(WriteRequest.class.getName(), Response.getDefaultInstance());
       registry.registerResponseInstance(ReadRequest.class.getName(), Response.getDefaultInstance());

       final RpcServer rpcServer = raftRpcFactory.createRpcServer(peerId.getEndpoint());
       RaftRpcServerFactory.addRaftRequestProcessors(rpcServer, raftExecutor, requestExecutor);

       rpcServer.registerProcessor(new MqttWriteRpcProcessor(server));
       rpcServer.registerProcessor(new MqttReadRpcProcessor(server));

       return rpcServer;
   }

    public void registerStateProcessor(StateProcessor processor) {
        stateProcessors.add(processor);
    }

    public void start() {
        String dataPath = metaConf.getRaftDataPath();
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");

        int eachProcessRaftGroupNum = metaConf.getRaftGroupNum();
        for (StateProcessor processor:stateProcessors) {

            List<RaftGroupHolder> raftGroupHolderList = multiRaftGroup.get(processor.groupCategory());
            if (raftGroupHolderList == null) {
                raftGroupHolderList = new ArrayList<>();
            }

            for (int i = 0; i < eachProcessRaftGroupNum; ++i) {
                String groupIdentity = wrapGroupName(processor.groupCategory(), i);

                Configuration groupConfiguration = initConf.copy();
                NodeOptions groupNodeOption = nodeOptions.copy();

                MqttStateMachine groupMqttStateMachine = new MqttStateMachine(this, processor, groupIdentity);
                groupNodeOption.setFsm(groupMqttStateMachine);
                groupNodeOption.setInitialConf(groupConfiguration);

                // to-do: snapshot
                int doSnapshotInterval = 0;
                groupNodeOption.setSnapshotIntervalSecs(doSnapshotInterval);

                // create raft group
                RaftGroupService raftGroupService = new RaftGroupService(groupIdentity, localPeerId, groupNodeOption, rpcServer, true);

                // start node
                Node node = raftGroupService.start(false);
                groupMqttStateMachine.setNode(node);
                RouteTable.getInstance().updateConfiguration(groupIdentity, groupConfiguration);

                // to-dp : a new node start, it can be added to this group, without restart running server. maybe need refresh route tables
                // add to multiRaftGroup
                raftGroupHolderList.add(new RaftGroupHolder(raftGroupService, groupMqttStateMachine, node));

                logger.info("create raft group, groupIdentity: {}", groupIdentity);
            }
        }
    }

    private String wrapGroupName(String category, int seq) {
        return category + "-" + seq;
    }

    public RaftGroupHolder getRaftGroupHolder(String groupId) throws Exception {
        String[] groupParam = groupId.split("%");
        if (groupParam.length != 2) {
            throw new Exception("Fail to get RaftGroupHolder");
        }

        return multiRaftGroup.get(groupParam[0]).get(Integer.parseInt(groupParam[1]));
    }

    public void applyOperation(Node node, Message data, FailoverClosure closure) {
        final Task task = new Task();
        MqttClosure mqttClosure = new MqttClosure(data, status -> {
            MqttClosure.MqttStatus mqttStatus = (MqttClosure.MqttStatus) status;
            closure.setThrowable(mqttStatus.getThrowable());
            closure.setResponse(mqttStatus.getResponse());
            closure.run(mqttStatus);
        });

        task.setData(ByteBuffer.wrap(data.toByteArray()));
        task.setDone(mqttClosure);
        node.apply(task);
    }

}
