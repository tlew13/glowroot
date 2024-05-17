package org.glowroot.ui;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Ticker;
import com.google.common.collect.*;
import com.google.common.io.CharStreams;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.glowroot.common.live.*;
import org.glowroot.common.model.Result;
import org.glowroot.common.util.ObjectMappers;
import org.glowroot.common2.repo.*;
import org.glowroot.common2.repo.util.HttpClient;
import org.glowroot.common2.repo.util.RollupLevelService;
import org.glowroot.wire.api.model.AggregateOuterClass;
import org.immutables.value.Value;
import org.glowroot.common.live.LiveJvmService;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.HOURS;

@JsonService
public class RegionRollupJsonService {

    private final ConfigRepository configRepository;
    private final TraceRepository traceRepository;
    private final LiveTraceRepository liveTraceRepository;
    private final ActiveAgentRepository activeAgentRepository;
    private final RollupLevelService rollupLevelService;
    private final TransactionCommonService transactionCommonService;
    private final LiveJvmService liveJvmService;
    private static final ObjectMapper mapper = ObjectMappers.create();
    private static final double NANOSECONDS_PER_MILLISECOND = 1000000.0;
    private static final JsonFactory jsonFactory = new JsonFactory();
    private final @Nullable Ticker ticker;


    public RegionRollupJsonService (boolean central, List<File> confDirs,
                                        ConfigRepository configRepository, TraceRepository traceRepository,
                                        LiveTraceRepository liveTraceRepository,
                                        ActiveAgentRepository activeAgentRepository,
                                        RollupLevelService rollupLevelService,
                                        TransactionCommonService transactionCommonService,
                                        LiveJvmService liveJvmService, @Nullable Ticker ticker,
                                        HttpClient httpClient, Properties props) throws Exception {
        this.configRepository = configRepository;
        this.traceRepository = traceRepository;
        this.liveTraceRepository = liveTraceRepository;
        this.activeAgentRepository = activeAgentRepository;
        this.rollupLevelService = rollupLevelService;
        this.transactionCommonService = transactionCommonService;
        this.liveJvmService = liveJvmService;
        this.ticker = ticker;
    }

    @GET(path = "/backend/error/region-trace-count", permission = "admin:view:regionErrorCount")
    String getRegionErrorCount(@BindRequest RegionCountDataRequest request) throws Exception {
        List<ActiveAgentRepository.AgentRollup> childAgents = activeAgentRepository.readActiveChildAgentRollups(
                request.agentRollupId(), request.from(), request.to());
        List<String> childAgentIds = getAgentIdsList(childAgents, request.agentRollupId());
        long errorCount = 0;
        for (String childAgentId : childAgentIds){
            if (childAgentId.contains("-" + request.region() + "-")){
                TraceRepository.TraceQuery query = ImmutableTraceQuery.builder()
                        .transactionType(request.transactionType())
                        .transactionName(request.transactionName())
                        .from(request.from())
                        .to(request.to())
                        .build();
                errorCount += traceRepository.readErrorCount(childAgentId, query);
            }
        }
        return Long.toString(errorCount);
    }

    @GET(path = "/backend/transaction/region-trace-count", permission = "admin:view:regionTraceCount")
    String getRegionSlowTraceCount(@BindRequest RegionCountDataRequest request) throws Exception {
        List<ActiveAgentRepository.AgentRollup> childAgents = activeAgentRepository.readActiveChildAgentRollups(
                request.agentRollupId(), request.from(), request.to());
        List<String> childAgentIds = getAgentIdsList(childAgents, request.agentRollupId());
        long slowTraceCount = 0;
        for (String childAgentId : childAgentIds){
            if (childAgentId.contains("-" + request.region() + "-")){
                TraceRepository.TraceQuery query = ImmutableTraceQuery.builder()
                        .transactionType(request.transactionType())
                        .transactionName(request.transactionName())
                        .from(request.from())
                        .to(request.to())
                        .build();
                slowTraceCount += traceRepository.readSlowCount(childAgentId, query);
                boolean includeActiveTraces = shouldIncludeActiveTraces(request);
                if (includeActiveTraces) {
                    slowTraceCount += liveTraceRepository.getMatchingTraceCount(request.transactionType(),
                            request.transactionName());
                }
            }
        }
        return Long.toString(slowTraceCount);
    }

    @GET(path = "/backend/error/region-points", permission = "agent:error:regionTraces")
    String getRegionErrorPoints(@BindAgentRollupId String agentRollupId,
                          @BindRequest RegionTracePointRequest request) throws Exception {
        return getPoints(LiveTraceRepository.TraceKind.ERROR, agentRollupId, request);
    }

    @GET(path = "/backend/transaction/region-points", permission = "agent:transaction:regionTraces")
    String getRegionTransactionPoints(@BindAgentRollupId String agentRollupId,
                                @BindRequest RegionTracePointRequest request) throws Exception {
        return getPoints(LiveTraceRepository.TraceKind.SLOW, agentRollupId, request);
    }

    @GET(path = "/backend/transaction/region-average", permission = "agent:transaction:regionOverview")
    String getRegionTransactionAverage(@BindAgentRollupId String agentRollupId,
                       @BindRequest RegionTransactionDataRequest request, @BindAutoRefresh boolean autoRefresh)
            throws Exception {
        /*
        Loop through all child rollups and aggregate transaction averages that contain "region"
         */
        List<ActiveAgentRepository.AgentRollup> childAgents = activeAgentRepository.readActiveChildAgentRollups(
                agentRollupId, request.from(), request.to());
        List<String> childAgentIds = getAgentIdsList(childAgents, agentRollupId);
        LiveAggregateRepository.AggregateQuery query = toChartQuery(request, RollupLevelService.DataKind.GENERAL);
        long liveCaptureTime = System.currentTimeMillis();
        List<LiveAggregateRepository.OverviewAggregate> combinedOverviewAggregates = Lists.newArrayList();
        for (String childAgentId : childAgentIds) {
            if (childAgentId.contains("-" + request.region() + "-")){
                List<LiveAggregateRepository.OverviewAggregate> overviewAggregates =
                        transactionCommonService.getOverviewAggregates(childAgentId, query, autoRefresh);
                if (overviewAggregates.isEmpty() && fallBackToLargestAggregates(query)) {
                    // fall back to largest aggregates in case expiration settings have recently changed
                    query = withLargestRollupLevel(query);
                    overviewAggregates = transactionCommonService.getOverviewAggregates(childAgentId,
                            query, autoRefresh);
                    if (!overviewAggregates.isEmpty() && ignoreFallBackData(query,
                            Iterables.getLast(overviewAggregates).captureTime())) {
                        // this is probably data from before the requested time period
                        overviewAggregates = ImmutableList.of();
                    }
                }
                // TODO more precise aggregate when from/to not on rollup grid
                List<LiveAggregateRepository.OverviewAggregate> overviewAggregatesForMerging = Lists.newArrayList();
                for (LiveAggregateRepository.OverviewAggregate overviewAggregate : overviewAggregates) {
                    long captureTime = overviewAggregate.captureTime();
                    if (captureTime > request.from() && captureTime <= request.to()) {
                        overviewAggregatesForMerging.add(overviewAggregate);
                    }
                }
                combinedOverviewAggregates.addAll(overviewAggregatesForMerging);
            }
        }

        List<LiveAggregateRepository.OverviewAggregate> averageOverviewAggregates = mergeOverviewAggregates(combinedOverviewAggregates);
        averageOverviewAggregates.sort((o1, o2) -> {
            Long captureTime1 = o1.captureTime();
            Long captureTime2 = o2.captureTime();
            return captureTime1.compareTo(captureTime2);
        });
        long dataPointIntervalMillis =
                configRepository.getRollupConfigs().get(query.rollupLevel()).intervalMillis();
        List<DataSeries> dataSeriesList = getDataSeriesForTimerChart(request, averageOverviewAggregates,
                dataPointIntervalMillis, liveCaptureTime);
        Map<Long, Long> transactionCounts = getTransactionCounts(averageOverviewAggregates);
        AggregateMerging.MergedAggregate mergedAggregate =
                AggregateMerging.getMergedAggregate(averageOverviewAggregates);

        StringBuilder sb = new StringBuilder();
        JsonGenerator jg = mapper.getFactory().createGenerator(CharStreams.asWriter(sb));
        try {
            jg.writeStartObject();
            jg.writeObjectField("dataSeries", dataSeriesList);
            jg.writeNumberField("dataPointIntervalMillis", dataPointIntervalMillis);
            jg.writeObjectField("transactionCounts", transactionCounts);
            jg.writeObjectField("mergedAggregate", mergedAggregate);
            jg.writeEndObject();
        } finally {
            jg.close();
        }
        return sb.toString();
    }

    @GET(path="/backend/jvm/region-jvm-status", permission="admin:view:regionJvmStatus")
    String getRegionJvmStatus(@BindRequest RegionJvmDataRequest request)throws Exception{
        List<ActiveAgentRepository.AgentRollup> childAgents = activeAgentRepository.readActiveChildAgentRollups(
                request.agentRollupId(), request.from(), request.to());
        List<String> childAgentIds = getAgentIdsList(childAgents, request.agentRollupId());
        int totalRegionJvmCount = 0;
        int countOfLiveJvms=0;
        int countOfDeadJvms=0;
        for(String childAgentId : childAgentIds){
            if (childAgentId.contains("-" + request.region() + "-")){
                totalRegionJvmCount++;
                boolean status=getJvmStatus(childAgentId);
                if(status){
                    countOfLiveJvms++;
                }else{
                    countOfDeadJvms++;
                }
            }
        }
        return "{\"total\":"+totalRegionJvmCount+
                ",\"live\":"+countOfLiveJvms+
                ",\"dead\":"+countOfDeadJvms+"}";
    }

    private List<String> getAgentIdsList(List<ActiveAgentRepository.AgentRollup> agents, String defaultAgentId){
        List<String> childAgentIds = Lists.newArrayList();
        for (ActiveAgentRepository.AgentRollup childAgent : agents){
            childAgentIds.add(childAgent.id());
        }
        if (childAgentIds.isEmpty()){
            childAgentIds.add(defaultAgentId);
        }
        return childAgentIds;
    }

    private boolean shouldIncludeActiveTraces(RegionCountDataRequest request) {

        long currentTimeMillis = System.currentTimeMillis();
        return (request.to() == 0 || request.to() > currentTimeMillis)
                && request.from() < currentTimeMillis;
    }

    private List<LiveAggregateRepository.OverviewAggregate> mergeOverviewAggregates(List<LiveAggregateRepository.OverviewAggregate> combinedOverviewAggregates){
        Map<Long, Double> durations = Maps.newHashMap();
        Map<Long, Long> counts = Maps.newHashMap();
        Map<Long, List<AggregateOuterClass.Aggregate.Timer>> mainThreadRootTimers = Maps.newHashMap();
        Map<Long, AggregateOuterClass.Aggregate.ThreadStats> mainThreadStats = Maps.newHashMap();
        Map<Long, Boolean> asyncTransactions = Maps.newHashMap();
        Map<Long, List<AggregateOuterClass.Aggregate.Timer>> asyncTimers = Maps.newHashMap();
        Map<Long, AggregateOuterClass.Aggregate.Timer> auxThreadRootTimers = Maps.newHashMap();
        Map<Long, AggregateOuterClass.Aggregate.ThreadStats> auxThreadStats = Maps.newHashMap();
        for (LiveAggregateRepository.OverviewAggregate overviewAggregate : combinedOverviewAggregates){
            if (!durations.containsKey(overviewAggregate.captureTime())){
                durations.put(overviewAggregate.captureTime(), overviewAggregate.totalDurationNanos());
                counts.put(overviewAggregate.captureTime(), overviewAggregate.transactionCount());
                mainThreadRootTimers.put(overviewAggregate.captureTime(), overviewAggregate.mainThreadRootTimers());
                mainThreadStats.put(overviewAggregate.captureTime(), overviewAggregate.mainThreadStats());
                asyncTransactions.put(overviewAggregate.captureTime(), overviewAggregate.asyncTransactions());
                asyncTimers.put(overviewAggregate.captureTime(), overviewAggregate.asyncTimers());
                auxThreadRootTimers.put(overviewAggregate.captureTime(), overviewAggregate.auxThreadRootTimer());
                auxThreadStats.put(overviewAggregate.captureTime(), overviewAggregate.auxThreadStats());
            }
            else {
                double oldDuration = durations.get(overviewAggregate.captureTime());
                durations.replace(overviewAggregate.captureTime(), overviewAggregate.totalDurationNanos()+oldDuration);
                long oldCount = counts.get(overviewAggregate.captureTime());
                counts.replace(overviewAggregate.captureTime(), overviewAggregate.transactionCount()+oldCount);
                List<AggregateOuterClass.Aggregate.Timer> savedMainThreadRootTimers = mainThreadRootTimers.get(overviewAggregate.captureTime());
                List<AggregateOuterClass.Aggregate.Timer> mergedMainThreadRootTimers = Lists.newArrayList();
                if (overviewAggregate.mainThreadRootTimers() != null){
                    mergedMainThreadRootTimers.addAll(savedMainThreadRootTimers);
                    mergedMainThreadRootTimers.addAll(overviewAggregate.mainThreadRootTimers());
                    mainThreadRootTimers.replace(overviewAggregate.captureTime(), mergedMainThreadRootTimers);
                }
                if (overviewAggregate.mainThreadStats() != null){
                    if (mainThreadStats.get(overviewAggregate.captureTime()) == null){
                        mainThreadStats.replace(overviewAggregate.captureTime(), overviewAggregate.mainThreadStats());
                    }
                    else {
                        AggregateOuterClass.Aggregate.ThreadStats mergedMainThreadStats = AggregateOuterClass.Aggregate.ThreadStats.newBuilder()
                                .mergeFrom(overviewAggregate.mainThreadStats())
                                .mergeFrom(mainThreadStats.get(overviewAggregate.captureTime()))
                                .build();
                        mainThreadStats.replace(overviewAggregate.captureTime(), mergedMainThreadStats);
                    }
                }
                if (overviewAggregate.asyncTransactions()){
                    asyncTransactions.replace(overviewAggregate.captureTime(), true);
                }
                List<AggregateOuterClass.Aggregate.Timer> savedAsyncTimers = asyncTimers.get(overviewAggregate.captureTime());
                List<AggregateOuterClass.Aggregate.Timer> mergedAsyncTimers = Lists.newArrayList();
                if (overviewAggregate.asyncTimers() != null){
                    mergedAsyncTimers.addAll(savedAsyncTimers);
                    mergedAsyncTimers.addAll(overviewAggregate.asyncTimers());
                    asyncTimers.replace(overviewAggregate.captureTime(), mergedAsyncTimers);
                }
                if (overviewAggregate.auxThreadRootTimer() != null){
                    if (auxThreadRootTimers.get(overviewAggregate.captureTime()) == null){
                        auxThreadRootTimers.replace(overviewAggregate.captureTime(), overviewAggregate.auxThreadRootTimer());
                    }
                    else {
                        AggregateOuterClass.Aggregate.Timer mergedAuxThreadRootTimer = AggregateOuterClass.Aggregate.Timer.newBuilder()
                                .mergeFrom(overviewAggregate.auxThreadRootTimer())
                                .mergeFrom(auxThreadRootTimers.get(overviewAggregate.captureTime()))
                                .build();
                        auxThreadRootTimers.replace(overviewAggregate.captureTime(), mergedAuxThreadRootTimer);
                    }
                }
                if (overviewAggregate.auxThreadStats() != null){
                    if (auxThreadStats.get(overviewAggregate.captureTime()) == null){
                        auxThreadStats.replace(overviewAggregate.captureTime(), overviewAggregate.auxThreadStats());
                    }
                    else {
                        AggregateOuterClass.Aggregate.ThreadStats mergedAuxThreadStats = AggregateOuterClass.Aggregate.ThreadStats.newBuilder()
                                .mergeFrom(overviewAggregate.auxThreadStats())
                                .mergeFrom(auxThreadStats.get(overviewAggregate.captureTime()))
                                .build();
                        auxThreadStats.replace(overviewAggregate.captureTime(), mergedAuxThreadStats);
                    }
                }
            }
        }
        List<LiveAggregateRepository.OverviewAggregate> averageOverviewAggregates = Lists.newArrayList();
        for (long captureTime : durations.keySet()){
            ImmutableOverviewAggregate.Builder builder = ImmutableOverviewAggregate.builder()
                    .captureTime(captureTime)
                    .totalDurationNanos(durations.get(captureTime))
                    .transactionCount(counts.get(captureTime))
                    .asyncTransactions(false)
                    .mainThreadRootTimers(mainThreadRootTimers.get(captureTime))
                    .mainThreadStats(mainThreadStats.get(captureTime))
                    .asyncTransactions(asyncTransactions.get(captureTime))
                    .asyncTimers(asyncTimers.get(captureTime))
                    .auxThreadRootTimer(auxThreadRootTimers.get(captureTime))
                    .auxThreadStats(auxThreadStats.get(captureTime));
            averageOverviewAggregates.add(builder.build());
        }
        return averageOverviewAggregates;
    }

    private String getPoints(LiveTraceRepository.TraceKind traceKind, String agentRollupId, RegionTracePointRequest request)
            throws Exception {
        double durationMillisLow = request.durationMillisLow();
        long durationNanosLow = Math.round(durationMillisLow * NANOSECONDS_PER_MILLISECOND);
        Long durationNanosHigh = null;
        Double durationMillisHigh = request.durationMillisHigh();
        if (durationMillisHigh != null) {
            durationNanosHigh = Math.round(durationMillisHigh * NANOSECONDS_PER_MILLISECOND);
        }
        TraceRepository.TraceQuery query = ImmutableTraceQuery.builder()
                .transactionType(request.transactionType())
                .transactionName(request.transactionName())
                .from(request.from())
                .to(request.to())
                .build();
        LiveTraceRepository.TracePointFilter filter = ImmutableTracePointFilter.builder()
                .durationNanosLow(durationNanosLow)
                .durationNanosHigh(durationNanosHigh)
                .headlineComparator(request.headlineComparator())
                .headline(request.headline())
                .errorMessageComparator(request.errorMessageComparator())
                .errorMessage(request.errorMessage())
                .userComparator(request.userComparator())
                .user(request.user())
                .attributeName(request.attributeName())
                .attributeValueComparator(request.attributeValueComparator())
                .attributeValue(request.attributeValue())
                .build();
        return new Handler(traceKind, agentRollupId, query, filter, request.limit()).handle(request);
    }

    private class Handler {

        private final LiveTraceRepository.TraceKind traceKind;
        private final String agentRollupId;
        private final TraceRepository.TraceQuery query;
        private final LiveTraceRepository.TracePointFilter filter;
        private final int limit;

        private Handler(LiveTraceRepository.TraceKind traceKind, String agentRollupId, TraceRepository.TraceQuery query,
                        LiveTraceRepository.TracePointFilter filter, int limit) {
            this.traceKind = traceKind;
            this.agentRollupId = agentRollupId;
            this.query = query;
            this.filter = filter;
            this.limit = limit;
        }

        private String handle(RegionTracePointRequest request) throws Exception {
            boolean captureActiveTracePoints = shouldCaptureActiveTracePoints();
            List<LiveTraceRepository.TracePoint> activeTracePoints = Lists.newArrayList();
            List<LiveTraceRepository.TracePoint> activeRegionTracePoints = Lists.newArrayList();
            long captureTime = 0;
            long captureTick = 0;
            if (captureActiveTracePoints && ticker != null) {
                captureTime = System.currentTimeMillis();
                captureTick = ticker.read();
                // capture active traces first to make sure that none are missed in the transition
                // between active and pending/stored (possible duplicates are removed below)
                activeTracePoints.addAll(liveTraceRepository.getMatchingActiveTracePoints(traceKind,
                        query.transactionType(), query.transactionName(), filter, limit,
                        captureTime, captureTick));
            }
            Result<LiveTraceRepository.TracePoint> queryResult =
                    getStoredAndPendingPoints(captureTime, captureActiveTracePoints);
            List<LiveTraceRepository.TracePoint> points = Lists.newArrayList(queryResult.records());
            List<LiveTraceRepository.TracePoint> regionPoints = Lists.newArrayList();
            removeDuplicatesBetweenActiveAndNormalTracePoints(activeTracePoints, points);
            int traceExpirationHours = configRepository.getStorageConfig().traceExpirationHours();
            boolean expired = points.isEmpty() && traceExpirationHours != 0 && query
                    .to() < System.currentTimeMillis() - HOURS.toMillis(traceExpirationHours);
            for (LiveTraceRepository.TracePoint activeTracePoint : activeTracePoints){
                if (activeTracePoint.agentId().contains("-" + request.region() + "-")){
                    activeRegionTracePoints.add(activeTracePoint);
                }
            }
            for (LiveTraceRepository.TracePoint point : points){
                if (point.agentId().contains("-" + request.region() + "-")){
                    regionPoints.add(point);
                }
            }
            return writeResponse(regionPoints, activeRegionTracePoints, queryResult.moreAvailable(), expired);
        }

        private boolean shouldCaptureActiveTracePoints() {
            long currentTimeMillis = System.currentTimeMillis();
            return (query.to() == 0 || query.to() > currentTimeMillis)
                    && query.from() < currentTimeMillis;
        }

        private Result<LiveTraceRepository.TracePoint> getStoredAndPendingPoints(long captureTime,
                                                                                 boolean captureActiveTraces) throws Exception {
            List<LiveTraceRepository.TracePoint> matchingPendingPoints;
            // it only seems worth looking at pending traces if request asks for active traces
            if (captureActiveTraces) {
                // important to grab pending traces before stored points to ensure none are
                // missed in the transition between pending and stored
                matchingPendingPoints = liveTraceRepository.getMatchingPendingPoints(traceKind,
                        query.transactionType(), query.transactionName(), filter, captureTime);
            } else {
                matchingPendingPoints = ImmutableList.of();
            }
            Result<LiveTraceRepository.TracePoint> queryResult;
            if (traceKind == LiveTraceRepository.TraceKind.SLOW) {
                queryResult = traceRepository.readSlowPoints(agentRollupId, query, filter, limit).get();
            } else {
                // TraceKind.ERROR
                queryResult = traceRepository.readErrorPoints(agentRollupId, query, filter, limit).get();
            }
            // create single merged and limited list of points
            List<LiveTraceRepository.TracePoint> orderedPoints = Lists.newArrayList(queryResult.records());
            for (LiveTraceRepository.TracePoint pendingPoint : matchingPendingPoints) {
                insertIntoOrderedPoints(pendingPoint, orderedPoints);
            }
            if (limit != 0 && orderedPoints.size() > limit) {
                orderedPoints = orderedPoints.subList(0, limit);
            }
            return new Result<LiveTraceRepository.TracePoint>(orderedPoints, queryResult.moreAvailable());
        }

        private void insertIntoOrderedPoints(LiveTraceRepository.TracePoint pendingPoint,
                                             List<LiveTraceRepository.TracePoint> orderedPoints) {
            int duplicateIndex = -1;
            int insertionIndex = -1;
            // check if duplicate and capture insertion index at the same time
            for (int i = 0; i < orderedPoints.size(); i++) {
                LiveTraceRepository.TracePoint point = orderedPoints.get(i);
                if (pendingPoint.traceId().equals(point.traceId())) {
                    duplicateIndex = i;
                    break;
                }
                if (pendingPoint.durationNanos() > point.durationNanos()) {
                    insertionIndex = i;
                    break;
                }
            }
            if (duplicateIndex != -1) {
                LiveTraceRepository.TracePoint point = orderedPoints.get(duplicateIndex);
                if (pendingPoint.durationNanos() > point.durationNanos()) {
                    // prefer the pending trace, it must be a partial trace that has just completed
                    orderedPoints.set(duplicateIndex, pendingPoint);
                }
                return;
            }
            if (insertionIndex == -1) {
                orderedPoints.add(pendingPoint);
            } else {
                orderedPoints.add(insertionIndex, pendingPoint);
            }
        }

        private void removeDuplicatesBetweenActiveAndNormalTracePoints(
                List<LiveTraceRepository.TracePoint> activeTracePoints, List<LiveTraceRepository.TracePoint> points) {
            for (Iterator<LiveTraceRepository.TracePoint> i = activeTracePoints.iterator(); i.hasNext();) {
                LiveTraceRepository.TracePoint activeTracePoint = i.next();
                for (Iterator<LiveTraceRepository.TracePoint> j = points.iterator(); j.hasNext();) {
                    LiveTraceRepository.TracePoint point = j.next();
                    if (!activeTracePoint.traceId().equals(point.traceId())) {
                        continue;
                    }
                    if (activeTracePoint.durationNanos() > point.durationNanos()) {
                        // prefer the active trace, it must be a partial trace that hasn't
                        // completed yet
                        j.remove();
                    } else {
                        // otherwise prefer the completed trace
                        i.remove();
                    }
                    // there can be at most one duplicate per id, so ok to break to outer
                    break;
                }
            }
        }

        private String writeResponse(List<LiveTraceRepository.TracePoint> points, List<LiveTraceRepository.TracePoint> activePoints,
                                     boolean limitExceeded, boolean expired) throws Exception {
            StringBuilder sb = new StringBuilder();
            JsonGenerator jg = jsonFactory.createGenerator(CharStreams.asWriter(sb));
            try {
                jg.writeStartObject();
                jg.writeArrayFieldStart("normalPoints");
                for (LiveTraceRepository.TracePoint point : points) {
                    if (!point.error() && !point.partial()) {
                        writePoint(point, jg);
                    }
                }
                jg.writeEndArray();
                jg.writeArrayFieldStart("errorPoints");
                for (LiveTraceRepository.TracePoint point : points) {
                    if (point.error() && !point.partial()) {
                        writePoint(point, jg);
                    }
                }
                jg.writeEndArray();
                jg.writeArrayFieldStart("partialPoints");
                for (LiveTraceRepository.TracePoint point : points) {
                    if (point.partial()) {
                        writePoint(point, jg);
                    }
                }
                for (LiveTraceRepository.TracePoint activePoint : activePoints) {
                    writePoint(activePoint, jg);
                }
                jg.writeEndArray();
                if (limitExceeded) {
                    jg.writeBooleanField("limitExceeded", true);
                }
                if (expired) {
                    jg.writeBooleanField("expired", true);
                }
                jg.writeEndObject();
            } finally {
                jg.close();
            }
            return sb.toString();
        }

        private void writePoint(LiveTraceRepository.TracePoint point, JsonGenerator jg) throws IOException {
            jg.writeStartArray();
            jg.writeNumber(point.captureTime());
            jg.writeNumber(point.durationNanos() / (double) NANOSECONDS_PER_MILLISECOND);
            jg.writeString(point.agentId());
            jg.writeString(point.traceId());
            if (point.checkLiveTraces()) {
                jg.writeBoolean(true);
            }
            jg.writeEndArray();
        }
    }

    @Value.Immutable
    public abstract static class RegionTracePointRequest {

        public abstract String region();
        public abstract String transactionType();
        public abstract @Nullable String transactionName();
        public abstract long from();
        public abstract long to();
        public abstract double durationMillisLow();
        public abstract @Nullable Double durationMillisHigh();
        public abstract @Nullable StringComparator headlineComparator();
        public abstract @Nullable String headline();
        public abstract @Nullable StringComparator errorMessageComparator();
        public abstract @Nullable String errorMessage();
        public abstract @Nullable StringComparator userComparator();
        public abstract @Nullable String user();
        public abstract @Nullable String attributeName();
        public abstract @Nullable StringComparator attributeValueComparator();
        public abstract @Nullable String attributeValue();

        public abstract int limit();
    }

    private LiveAggregateRepository.AggregateQuery toChartQuery(RegionTransactionDataRequest request, RollupLevelService.DataKind dataKind) throws Exception {
        int rollupLevel =
                rollupLevelService.getRollupLevelForView(request.from(), request.to(), dataKind);
        long rollupIntervalMillis =
                configRepository.getRollupConfigs().get(rollupLevel).intervalMillis();
        // read the closest rollup to the left and right of chart, in order to display line sloping
        // correctly off the chart to the left and right
        long from = RollupLevelService.getFloorRollupTime(request.from(), rollupIntervalMillis);
        long to = RollupLevelService.getCeilRollupTime(request.to(), rollupIntervalMillis);
        return ImmutableAggregateQuery.builder()
                .transactionType(request.transactionType())
                .transactionName(request.transactionName())
                .from(from)
                .to(to)
                .rollupLevel(rollupLevel)
                .build();
    }

    private boolean fallBackToLargestAggregates(LiveAggregateRepository.AggregateQuery query) {
        return query.rollupLevel() < getLargestRollupLevel()
                && query.from() < System.currentTimeMillis() - getLargestRollupIntervalMillis() * 2;
    }

    private int getLargestRollupLevel() {
        return configRepository.getRollupConfigs().size() - 1;
    }

    private long getLargestRollupIntervalMillis() {
        List<ConfigRepository.RollupConfig> rollupConfigs = configRepository.getRollupConfigs();
        return rollupConfigs.get(rollupConfigs.size() - 1).intervalMillis();
    }

    private LiveAggregateRepository.AggregateQuery withLargestRollupLevel(LiveAggregateRepository.AggregateQuery query) {
        return ImmutableAggregateQuery.builder()
                .copyFrom(query)
                .rollupLevel(getLargestRollupLevel())
                .build();
    }

    private boolean ignoreFallBackData(LiveAggregateRepository.AggregateQuery query, long lastCaptureTime) {
        return lastCaptureTime < query.from() + getLargestRollupIntervalMillis();
    }

    private static List<DataSeries> getDataSeriesForTimerChart(RegionTransactionDataRequest request,
                                                               List<LiveAggregateRepository.OverviewAggregate> aggregates, long dataPointIntervalMillis,
                                                               long liveCaptureTime) {
        if (aggregates.isEmpty()) {
            return Lists.newArrayList();
        }
        List<StackedPoint> stackedPoints = Lists.newArrayList();
        for (LiveAggregateRepository.OverviewAggregate aggregate : aggregates) {
            stackedPoints.add(StackedPoint.create(aggregate));
        }
        return getTimerDataSeries(request, stackedPoints, dataPointIntervalMillis, liveCaptureTime);
    }

    private static List<DataSeries> getTimerDataSeries(RegionTransactionDataRequest request,
                                                       List<StackedPoint> stackedPoints, long dataPointIntervalMillis, long liveCaptureTime) {
        DataSeriesHelper dataSeriesHelper =
                new DataSeriesHelper(liveCaptureTime, dataPointIntervalMillis);
        final int topX = 5;
        List<String> timerNames = getTopTimerNames(stackedPoints, topX + 1);
        List<DataSeries> dataSeriesList = Lists.newArrayList();
        for (int i = 0; i < Math.min(timerNames.size(), topX); i++) {
            dataSeriesList.add(new DataSeries(timerNames.get(i)));
        }
        // need 'other' data series even if < topX timers in order to capture root timers,
        // e.g. time spent in 'servlet' timer but not in any nested timer
        DataSeries otherDataSeries = new DataSeries(null);
        LiveAggregateRepository.OverviewAggregate priorOverviewAggregate = null;
        for (StackedPoint stackedPoint : stackedPoints) {
            LiveAggregateRepository.OverviewAggregate overviewAggregate = stackedPoint.getOverviewAggregate();
            if (priorOverviewAggregate == null) {
                // first aggregate
                dataSeriesHelper.addInitialUpslopeIfNeeded(request.from(),
                        overviewAggregate.captureTime(), dataSeriesList, otherDataSeries);
            } else {
                dataSeriesHelper.addGapIfNeeded(priorOverviewAggregate.captureTime(),
                        overviewAggregate.captureTime(), dataSeriesList, otherDataSeries);
            }
            MutableDoubleMap<String> stackedTimers = stackedPoint.getStackedTimers();
            double totalOtherNanos = overviewAggregate.totalDurationNanos();
            for (DataSeries dataSeries : dataSeriesList) {
                MutableDouble totalNanos = stackedTimers.get(dataSeries.getName());
                if (totalNanos == null) {
                    dataSeries.add(overviewAggregate.captureTime(), 0);
                } else {
                    // convert to average milliseconds
                    double value = (totalNanos.doubleValue() / overviewAggregate.transactionCount())
                            / NANOSECONDS_PER_MILLISECOND;
                    dataSeries.add(overviewAggregate.captureTime(), value);
                    totalOtherNanos -= totalNanos.doubleValue();
                }
            }
            if (overviewAggregate.transactionCount() == 0) {
                otherDataSeries.add(overviewAggregate.captureTime(), 0);
            } else {
                // convert to average milliseconds
                otherDataSeries.add(overviewAggregate.captureTime(),
                        (totalOtherNanos / overviewAggregate.transactionCount())
                                / NANOSECONDS_PER_MILLISECOND);
            }
            priorOverviewAggregate = overviewAggregate;
        }
        if (priorOverviewAggregate != null) {
            dataSeriesHelper.addFinalDownslopeIfNeeded(dataSeriesList, otherDataSeries,
                    priorOverviewAggregate.captureTime());
        }
        dataSeriesList.add(otherDataSeries);
        return dataSeriesList;
    }

    private static Map<Long, Long> getTransactionCounts(
            List<LiveAggregateRepository.OverviewAggregate> overviewAggregates) {
        Map<Long, Long> transactionCounts = Maps.newHashMap();
        for (LiveAggregateRepository.OverviewAggregate overviewAggregate : overviewAggregates) {
            transactionCounts.put(overviewAggregate.captureTime(),
                    overviewAggregate.transactionCount());
        }
        return transactionCounts;
    }

    private static List<String> getTopTimerNames(List<StackedPoint> stackedPoints, int topX) {
        MutableDoubleMap<String> timerTotals = new MutableDoubleMap<String>();
        for (StackedPoint stackedPoint : stackedPoints) {
            for (Map.Entry<String, MutableDouble> entry : stackedPoint.getStackedTimers()
                    .entrySet()) {
                timerTotals.add(entry.getKey(), entry.getValue().doubleValue());
            }
        }
        Ordering<Map.Entry<String, MutableDouble>> valueOrdering =
                Ordering.natural()
                        .onResultOf(new Function<Map.Entry<String, MutableDouble>, Double>() {
                            @Override
                            public Double apply(
                                    Map. /*@Nullable*/ Entry<String, MutableDouble> entry) {
                                checkNotNull(entry);
                                return entry.getValue().doubleValue();
                            }
                        });
        List<String> timerNames = Lists.newArrayList();
        @SuppressWarnings("assignment.type.incompatible")
        List<Map.Entry<String, MutableDouble>> topTimerTotals =
                valueOrdering.greatestOf(timerTotals.entrySet(), topX);
        for (Map.Entry<String, MutableDouble> entry : topTimerTotals) {
            timerNames.add(entry.getKey());
        }
        return timerNames;
    }

    interface CountRequestBase {
        String region();
        String agentRollupId();
        String transactionType();
        @Nullable
        String transactionName();
        long from();
        long to();
    }

    @Value.Immutable
    interface RegionCountDataRequest extends CountRequestBase {}

    interface TransactionRequestBase {
        String region();
        String transactionType();
        @Nullable
        String transactionName();
        long from();
        long to();
    }

    @Value.Immutable
    interface RegionTransactionDataRequest extends TransactionRequestBase {}

    private static class StackedPoint {

        private final LiveAggregateRepository.OverviewAggregate overviewAggregate;
        // stacked timer values only include time spent as a leaf node in the timer tree
        private final MutableDoubleMap<String> stackedTimers;

        private static StackedPoint create(LiveAggregateRepository.OverviewAggregate overviewAggregate) {
            MutableDoubleMap<String> stackedTimers = new MutableDoubleMap<String>();
            for (AggregateOuterClass.Aggregate.Timer rootTimer : overviewAggregate.mainThreadRootTimers()) {
                // skip root timers
                for (AggregateOuterClass.Aggregate.Timer topLevelTimer : rootTimer.getChildTimerList()) {
                    // traverse tree starting at top-level (under root) timers
                    addToStackedTimer(topLevelTimer, stackedTimers);
                }
            }
            return new StackedPoint(overviewAggregate, stackedTimers);
        }

        private StackedPoint(LiveAggregateRepository.OverviewAggregate overviewAggregate,
                             MutableDoubleMap<String> stackedTimers) {
            this.overviewAggregate = overviewAggregate;
            this.stackedTimers = stackedTimers;
        }

        private LiveAggregateRepository.OverviewAggregate getOverviewAggregate() {
            return overviewAggregate;
        }

        private MutableDoubleMap<String> getStackedTimers() {
            return stackedTimers;
        }

        private static void addToStackedTimer(AggregateOuterClass.Aggregate.Timer timer,
                                              MutableDoubleMap<String> stackedTimers) {
            double totalNestedNanos = 0;
            for (AggregateOuterClass.Aggregate.Timer childTimer : timer.getChildTimerList()) {
                totalNestedNanos += childTimer.getTotalNanos();
                addToStackedTimer(childTimer, stackedTimers);
            }
            String timerName = timer.getName();
            stackedTimers.add(timerName, timer.getTotalNanos() - totalNestedNanos);
        }
    }

    private static class MutableDoubleMap<K> extends HashMap<K, MutableDouble> {
        private void add(K key, double delta) {
            MutableDouble existing = get(key);
            if (existing == null) {
                put(key, new MutableDouble(delta));
            } else {
                existing.value += delta;
            }
        }
    }

    private static class MutableDouble {
        private double value;
        private MutableDouble(double value) {
            this.value = value;
        }
        private double doubleValue() {
            return value;
        }
    }

    private boolean getJvmStatus(String id)throws Exception{
        return liveJvmService.isAvailable(id);
    }

    interface JvmRequestBase {
        String agentRollupId();
        String region();
        long from();
        long to();
    }

    @Value.Immutable
    interface RegionJvmDataRequest extends JvmRequestBase {}

}


