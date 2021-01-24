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

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.SessionFactory;
import com.pushtechnology.diffusion.datatype.Bytes;
import com.pushtechnology.diffusion.client.features.Topics;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import static java.util.Arrays.asList;

import java.util.List;

public class TopicCopy {

    private final String url;
    private final String principal;
    private final String credentials;
    private final String sourceSelector;
    private final String targetBranch;
    private final List<String> removeProperties;

    private Session session;
    private Topics topics;
    private GenericValueStream stream;
    
    public TopicCopy(final OptionSet options) {
        url = (String) options.valueOf("url");
        principal = options.has("principal") ? (String)options.valueOf("principal") : null;
        credentials = options.has("credentials") ? (String)options.valueOf("credentials") : null;
        sourceSelector = (String)options.valueOf("source");
        targetBranch = (String)options.valueOf("target");
        removeProperties = List.of(((String)options.valueOf("remove")).split(","));
    }

    public void connect() {
        SessionFactory factory = Diffusion.sessions();
        if(principal != null) {
            factory = factory.principal(principal);
        }
        if(credentials != null) {
            factory = factory.password(credentials);
        }

        while (true) {
            session = factory.open(url);
            if(session != null && session.getState().isConnected()) {
                break; // Connected
            }
            System.err.println("Unable to connect, retrying");
            try {
                Thread.sleep(1000);
            }
            catch(InterruptedException ignore) {
            }
        }

        System.out.println("Connected: " + session.getSessionId());

        topics = session.feature(Topics.class);
    }

    public void run() {
        stream = new GenericValueStream(session, targetBranch, removeProperties);
        topics.addStream(sourceSelector, Bytes.class, stream);

        topics.subscribe(sourceSelector);

        while(session != null && !session.getState().isClosed()) {
            try {
                Thread.sleep(1000);
            }
            catch(Exception ignore) {
            }
        }
    }

    public void stop() {
        if(stream != null) {
            topics.removeStream(stream);
        }
        if(session != null) {
            session.close();
        }
    }
    
    public static void main(final String[] args) throws Exception {

        final OptionParser optionParser = new OptionParser() {
            {
                acceptsAll(asList("h", "help"), "This help");

                acceptsAll(asList("u", "url"), "Diffusion URL")
                    .withRequiredArg()
                    .ofType(String.class)
                    .defaultsTo("ws://localhost:8080");

                acceptsAll(asList("p", "principal"), "Diffusion principal (username)")
                    .withRequiredArg()
                    .ofType(String.class)
                    .defaultsTo("control");

                acceptsAll(asList("c", "credentials"), "Diffusion credentials (password)")
                    .withRequiredArg()
                    .ofType(String.class)
                    .defaultsTo("password");

                acceptsAll(asList("source"), "Source topic selector")
                    .withRequiredArg()
                    .ofType(String.class);

                acceptsAll(asList("target"), "Target topic branch")
                    .withRequiredArg()
                    .ofType(String.class);

                acceptsAll(asList("remove"), "Comma-separated list of properties to remove")
                    .withRequiredArg()
                    .ofType(String.class)
                    .defaultsTo("_VIEW,REMOVAL");
            }
            };
        
        OptionSet options = optionParser.parse(args);
        if(options.has("help")) {
            optionParser.printHelpOn(System.out);
            System.exit(0);
        }

        TopicCopy app = new TopicCopy(options);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    app.stop();
        }));
        
        app.connect();
        app.run();

        app.stop();
    }
}
