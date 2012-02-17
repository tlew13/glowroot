/**
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.informantproject.local.ui;

import org.informantproject.local.trace.TraceDao;
import org.informantproject.local.ui.HttpServer.JsonService;
import org.informantproject.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Json service to clear captured data. Bound to url "/admin" in HttpServer.
 * 
 * @author Trask Stalnaker
 * @since 0.5
 */
@Singleton
public class AdminJsonService implements JsonService {

    private static final Logger logger = LoggerFactory.getLogger(AdminJsonService.class);

    private final TraceDao traceDao;
    private final Clock clock;

    @Inject
    public AdminJsonService(TraceDao traceDao, Clock clock) {
        this.traceDao = traceDao;
        this.clock = clock;
    }

    // called dynamically from HttpServer
    public void handleCleardata(String message) {
        logger.debug("handleCleardata(): message={}", message);
        JsonObject request = new JsonParser().parse(message).getAsJsonObject();
        long keepMillis = request.get("keepMillis").getAsLong();
        if (keepMillis == 0) {
            traceDao.deleteAllStoredTraces();
        } else {
            traceDao.deleteStoredTraces(0, clock.currentTimeMillis() - keepMillis);
        }
    }
}
