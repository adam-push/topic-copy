/*
 * Copyright (C) 2021 Push Technology Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.pushtechnology.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.ErrorReason;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.features.Topics.UnsubscribeReason;
import com.pushtechnology.diffusion.client.features.Topics.ValueStream;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.datatype.Bytes;
import com.pushtechnology.diffusion.datatype.binary.Binary;
import com.pushtechnology.diffusion.datatype.json.JSON;

public class GenericValueStream implements ValueStream<Bytes> {

    private final TopicControl topicControl;
    private final TopicUpdate topicUpdate;
    private final String targetBranch;
    private final int trimPathLength;
    private final List<String> removeProperties;

    public GenericValueStream(Session session, String targetBranch, int trimPathLength, List<String> removeProperties) {
        this.topicControl = session.feature(TopicControl.class);
        this.topicUpdate = session.feature(TopicUpdate.class);
        this.targetBranch = targetBranch;
        this.trimPathLength = trimPathLength;
        this.removeProperties = removeProperties;
    }

    @Override
    public void onSubscription(String topicPath, TopicSpecification specification) {
        System.out.println("Subscribed to " + topicPath);

        String[] srcTopicParts = topicPath.split("/");
        if (trimPathLength < srcTopicParts.length) {
            srcTopicParts = Arrays.copyOfRange(srcTopicParts, trimPathLength, srcTopicParts.length);
        }

        String newTopic = String.join("/",
                                      Stream.of(targetBranch.split("/"), srcTopicParts)
                                      .flatMap(Stream::of)
                                      .toArray(String[]::new));

        Map<String, String> properties = new HashMap<String, String>(specification.getProperties());
        removeProperties.forEach(prop -> {
                properties.remove(prop);
            });

        TopicSpecification newSpec = Diffusion.newTopicSpecification(specification.getType());
        newSpec = newSpec.withProperties(properties);

        System.out.println("Creating topic: " + newTopic);
        System.out.println(newSpec);

        topicControl.addTopic(newTopic, newSpec).whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Add " + newTopic + " error: " + ex.getMessage());
            }
            else {
                System.out.println("Add " + newTopic + ": " + result);
            }
        });
    }

    @Override
    public void onUnsubscription(String topicPath, TopicSpecification specification, UnsubscribeReason reason) {
        System.out.println("Unsubscribed from " + topicPath);
    }

    @Override
    public void onClose() {
        System.out.println("GenericValueStream closed");
    }

    @Override
    public void onError(ErrorReason errorReason) {
        System.out.println("Error on GenericValueStream: " + errorReason.toString());
    }

    @Override
    public void onValue(String topicPath, TopicSpecification specification, Bytes oldValue, Bytes newValue) {
        String newPath = targetBranch + "/" + topicPath;
        Class clz = null;
        switch(specification.getType()) {
        case STRING:
            clz = String.class;
            break;
        case JSON:
            clz = JSON.class;
            break;
        case INT64:
            clz = Long.class;
            break;
        case DOUBLE:
            clz = Double.class;
            break;
        case BINARY:
            clz = Binary.class;
            break;
        default:
            break;
        }

        if(clz == null) {
            System.err.println("Unable to determine class from type: " + specification.getType());
            return;
        }

        topicUpdate.set(newPath, clz, newValue);
    }
}
