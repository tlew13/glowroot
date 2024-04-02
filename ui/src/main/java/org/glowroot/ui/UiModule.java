/*
 * Copyright 2011-2023 the original author or authors.
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
package org.glowroot.ui;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.glowroot.common.util.PropertiesFiles;
import org.immutables.builder.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.glowroot.common.live.LiveAggregateRepository;
import org.glowroot.common.live.LiveJvmService;
import org.glowroot.common.live.LiveTraceRepository;
import org.glowroot.common.live.LiveWeavingService;
import org.glowroot.common.util.Clock;
import org.glowroot.common2.config.EmbeddedWebConfig;
import org.glowroot.common2.repo.ActiveAgentRepository;
import org.glowroot.common2.repo.AgentDisplayRepository;
import org.glowroot.common2.repo.AggregateRepository;
import org.glowroot.common2.repo.AlertingDisabledRepository;
import org.glowroot.common2.repo.ConfigRepository;
import org.glowroot.common2.repo.EnvironmentRepository;
import org.glowroot.common2.repo.GaugeValueRepository;
import org.glowroot.common2.repo.IncidentRepository;
import org.glowroot.common2.repo.RepoAdmin;
import org.glowroot.common2.repo.SyntheticResultRepository;
import org.glowroot.common2.repo.TraceAttributeNameRepository;
import org.glowroot.common2.repo.TraceRepository;
import org.glowroot.common2.repo.TransactionTypeRepository;
import org.glowroot.common2.repo.util.HttpClient;
import org.glowroot.common2.repo.util.MailService;
import org.glowroot.common2.repo.util.RollupLevelService;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class UiModule {

    private static final Logger logger = LoggerFactory.getLogger(UiModule.class);

    // non-null when using netty
    private final @Nullable HttpServer httpServer;

    // CommonHandler is non-null when using servlet container (applies to central only)
    private final @Nullable CommonHandler commonHandler;

    private final ExecutorService reportingExecutor;

    @Builder.Factory
    public static UiModule createUiModule(
            boolean central,
            boolean servlet,
            boolean offlineViewer,
            boolean webPortReadOnly, // only used for embedded
            @Nullable String bindAddress, // only used for central
            @Nullable Integer port, // only used for central
            @Nullable Boolean https, // only used for central
            @Nullable String contextPath, // only used for central
            List<File> confDirs,
            File logDir,
            Pattern logFileNamePattern,
            @Nullable Ticker ticker, // @Nullable to deal with shading from glowroot server
            Clock clock,
            @Nullable LiveJvmService liveJvmService,
            AgentDisplayRepository agentDisplayRepository,
            final ConfigRepository configRepository,
            AlertingDisabledRepository alertingDisabledRepository,
            ActiveAgentRepository activeAgentRepository,
            EnvironmentRepository environmentRepository,
            TransactionTypeRepository transactionTypeRepository,
            AggregateRepository aggregateRepository,
            TraceAttributeNameRepository traceAttributeNameRepository,
            TraceRepository traceRepository,
            GaugeValueRepository gaugeValueRepository,
            @Nullable SyntheticResultRepository syntheticResultRepository, // null for embedded
            IncidentRepository incidentRepository,
            RepoAdmin repoAdmin,
            RollupLevelService rollupLevelService,
            LiveTraceRepository liveTraceRepository,
            LiveAggregateRepository liveAggregateRepository,
            @Nullable LiveWeavingService liveWeavingService,
            SessionMapFactory sessionMapFactory,
            HttpClient httpClient,
            int numWorkerThreads,
            String version) throws Exception {

        TransactionCommonService transactionCommonService = new TransactionCommonService(
                aggregateRepository, liveAggregateRepository, configRepository, clock);
        TraceCommonService traceCommonService = new TraceCommonService(traceRepository,
                liveTraceRepository, agentDisplayRepository);
        ErrorCommonService errorCommonService =
                new ErrorCommonService(aggregateRepository, liveAggregateRepository);
        MailService mailService = new MailService();

        AdminJsonService adminJsonService = new AdminJsonService(central, offlineViewer,
                webPortReadOnly, confDirs, configRepository, repoAdmin, liveAggregateRepository,
                mailService, httpClient);

        LayoutService layoutService = new LayoutService(central, offlineViewer, version,
                agentDisplayRepository, configRepository, transactionTypeRepository,
                traceAttributeNameRepository, environmentRepository, liveAggregateRepository,
                liveTraceRepository);

        ExecutorService reportingExecutor;
        if (central) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("Glowroot-UI-Report-Worker")
                    .build();
            reportingExecutor = new ThreadPoolExecutor(0, 10, 60, SECONDS,
                    new LinkedBlockingQueue<Runnable>(), threadFactory);
        } else {
            reportingExecutor = MoreExecutors.newDirectExecutorService();
        }

        List<Object> jsonServices = Lists.newArrayList();
        jsonServices.add(new LayoutJsonService(activeAgentRepository, layoutService));
        jsonServices.add(new TransactionJsonService(transactionCommonService, traceCommonService,
                aggregateRepository, configRepository, rollupLevelService, clock));
        jsonServices.add(new TracePointJsonService(traceRepository, liveTraceRepository,
                configRepository, ticker, clock));
        jsonServices.add(new TraceJsonService(traceCommonService));
        jsonServices.add(new ErrorJsonService(errorCommonService, transactionCommonService,
                traceRepository, configRepository, rollupLevelService, clock));
        jsonServices.add(new GaugeValueJsonService(gaugeValueRepository, rollupLevelService,
                configRepository));
        jsonServices
                .add(new JvmJsonService(environmentRepository, configRepository, liveJvmService));
        jsonServices.add(new IncidentJsonService(central, incidentRepository,
                agentDisplayRepository, configRepository, syntheticResultRepository, clock));
        jsonServices.add(new ReportJsonService(agentDisplayRepository, configRepository,
                activeAgentRepository, transactionTypeRepository, aggregateRepository,
                gaugeValueRepository, liveAggregateRepository, rollupLevelService,
                reportingExecutor));
        jsonServices.add(new ConfigJsonService(transactionTypeRepository, gaugeValueRepository,
                liveAggregateRepository, configRepository));
        jsonServices.add(new AlertConfigJsonService(configRepository, alertingDisabledRepository,
                gaugeValueRepository, syntheticResultRepository, clock, central));
        jsonServices.add(new UserConfigJsonService(configRepository));
        jsonServices
                .add(new RoleConfigJsonService(central, configRepository, activeAgentRepository));
        jsonServices.add(new GaugeConfigJsonService(configRepository, liveJvmService));
        jsonServices.add(new InstrumentationConfigJsonService(central, configRepository,
                liveWeavingService, liveJvmService));
        jsonServices.add(adminJsonService);
        //load api-plugin JsonServices
        Path uiBase = Paths.get(new File(".").getCanonicalPath(), "ui");
        Path apiPluginConfFile = Paths.get(uiBase.toString(),"api-plugin.conf");
        Path apiPluginPropertiesFile = Paths.get(uiBase.toString(),"api-plugin.properties");
        if (Files.exists(apiPluginConfFile)) {
            Properties props = null;
            if (Files.exists(apiPluginPropertiesFile)){
                props = PropertiesFiles.load(apiPluginPropertiesFile.toFile());
            }
            loadApiPluginJsonServices(apiPluginConfFile, central, confDirs, configRepository,
                    traceRepository, liveTraceRepository, activeAgentRepository, rollupLevelService,
                    transactionCommonService, httpClient, jsonServices, props);
        }

        if (central) {
            checkNotNull(syntheticResultRepository);
            jsonServices.add(new SyntheticResultJsonService(syntheticResultRepository,
                    rollupLevelService, configRepository, clock));
            jsonServices.add(new SyntheticMonitorConfigJsonService(configRepository));
        }

        HttpSessionManager httpSessionManager = new HttpSessionManager(central, offlineViewer,
                configRepository, clock, layoutService, sessionMapFactory);
        IndexHtmlHttpService indexHtmlHttpService = new IndexHtmlHttpService(layoutService);
        TraceDetailHttpService traceDetailHttpService =
                new TraceDetailHttpService(traceCommonService);
        TraceExportHttpService traceExportHttpService =
                new TraceExportHttpService(traceCommonService, version);
        GlowrootLogHttpService glowrootLogHttpService =
                new GlowrootLogHttpService(logDir, logFileNamePattern);
        HealthCheckHttpService healthCheckHttpService = new HealthCheckHttpService(repoAdmin);

        Map<Pattern, HttpService> httpServices = Maps.newHashMap();
        // http services
        httpServices.put(Pattern.compile("^/$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/transaction/.*$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/error/.*$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/jvm/.*$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/incidents$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/report/.*$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/config/.*$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/admin/.*$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/profile/.*$"), indexHtmlHttpService);
        httpServices.put(Pattern.compile("^/login$"), indexHtmlHttpService);
        // export service is not bound under /backend since the export url is visible to users
        // as the download url for the export file
        httpServices.put(Pattern.compile("^/export/trace$"), traceExportHttpService);
        httpServices.put(Pattern.compile("^/backend/trace/entries$"), traceDetailHttpService);
        httpServices.put(Pattern.compile("^/backend/trace/queries$"), traceDetailHttpService);
        httpServices.put(Pattern.compile("^/backend/trace/main-thread-profile$"),
                traceDetailHttpService);
        httpServices.put(Pattern.compile("^/backend/trace/aux-thread-profile$"),
                traceDetailHttpService);
        httpServices.put(Pattern.compile("^/log$"), glowrootLogHttpService);
        httpServices.put(Pattern.compile("^/health$"), healthCheckHttpService);

        if (central) {
            httpServices.put(Pattern.compile("^/synthetic-monitors$"), indexHtmlHttpService);
        }

        CommonHandler commonHandler = new CommonHandler(central, layoutService, httpServices,
                httpSessionManager, jsonServices, clock);

        if (servlet) {
            return new UiModule(commonHandler, reportingExecutor);
        } else {
            HttpServer httpServer;
            int initialPort;
            if (central) {
                httpServer = new HttpServer(checkNotNull(bindAddress), checkNotNull(https),
                        Suppliers.ofInstance(checkNotNull(contextPath)), numWorkerThreads,
                        commonHandler, confDirs, central, offlineViewer);
                initialPort = checkNotNull(port);
            } else {
                final EmbeddedWebConfig initialWebConfig = configRepository.getEmbeddedWebConfig();
                Supplier<String> contextPathSupplier = new Supplier<String>() {
                    @Override
                    public String get() {
                        try {
                            return configRepository.getEmbeddedWebConfig().contextPath();
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            return initialWebConfig.contextPath();
                        }
                    }
                };
                httpServer = new HttpServer(initialWebConfig.bindAddress(),
                        initialWebConfig.https(), contextPathSupplier, numWorkerThreads,
                        commonHandler, confDirs, central, offlineViewer);
                initialPort = initialWebConfig.port();
            }
            adminJsonService.setHttpServer(httpServer);
            httpServer.bindEventually(initialPort);
            return new UiModule(httpServer, reportingExecutor);
        }
    }

    private UiModule(HttpServer httpServer, ExecutorService reportingExecutor) {
        this.httpServer = httpServer;
        commonHandler = null;
        this.reportingExecutor = reportingExecutor;
    }

    private UiModule(CommonHandler commonHandler, ExecutorService reportingExecutor) {
        this.commonHandler = commonHandler;
        httpServer = null;
        this.reportingExecutor = reportingExecutor;
    }

    public CommonHandler getCommonHandler() {
        // only called when using servlet container
        return checkNotNull(commonHandler);
    }

    // used by tests and by central ui
    public void close(boolean jvmTermination) throws Exception {
        if (httpServer != null) {
            httpServer.close();
        }
        if (!jvmTermination) {
            reportingExecutor.shutdown();
        }
    }

    private static void loadApiPluginJsonServices(Path apiPluginConfFile, boolean central, List<File> confDirs,
                                                  ConfigRepository configRepository, TraceRepository traceRepository,
                                                  LiveTraceRepository liveTraceRepository,
                                                  ActiveAgentRepository activeAgentRepository,
                                                  RollupLevelService rollupLevelService,
                                                  TransactionCommonService transactionCommonService,
                                                  HttpClient httpClient,
                                                  List<Object> jsonServices, Properties props)
            throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        byte [] apiPluginConfContents = Files.readAllBytes(apiPluginConfFile);
        ObjectMapper mapper = new ObjectMapper();
        List<ApiPluginConf> ApiPluginConfList = mapper.readValue(apiPluginConfContents, new TypeReference<List<ApiPluginConf>>() { });
        for (ApiPluginConf apiPluginConf : ApiPluginConfList) {
            String pluginClassName = apiPluginConf.getClassName();
            Class<?> pluginClass = Class.forName(pluginClassName);
            Constructor<?> constructor = pluginClass.getConstructor(boolean.class, List.class, ConfigRepository.class,
                    TraceRepository.class, LiveTraceRepository.class, ActiveAgentRepository.class, RollupLevelService.class,
                    TransactionCommonService.class, HttpClient.class, Properties.class);
            Object object = constructor.newInstance(new Object[] { central, confDirs, configRepository, traceRepository,
                    liveTraceRepository, activeAgentRepository, rollupLevelService, transactionCommonService,
                    httpClient, props });
            jsonServices.add(object);
        }
    }
}
