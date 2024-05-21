package org.glowroot.ui;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Ticker;
import com.google.common.collect.*;
import com.google.common.io.CharStreams;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.glowroot.common.live.*;
import org.glowroot.common.util.ObjectMappers;
import org.glowroot.common2.repo.ActiveAgentRepository;
import org.glowroot.common2.repo.ConfigRepository;
import org.glowroot.common2.repo.ImmutableTraceQuery;
import org.glowroot.common2.repo.TraceRepository;
import org.glowroot.common2.repo.util.HttpClient;
import org.glowroot.common2.repo.util.RollupLevelService;
import org.glowroot.wire.api.model.AggregateOuterClass;
import org.immutables.value.Value;
import org.glowroot.common.live.LiveJvmService;

import java.io.File;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

@JsonService
public class AggregateMetricsJsonService {

    private final ConfigRepository configRepository;
    private final TraceRepository traceRepository;
    private final LiveTraceRepository liveTraceRepository;
    private final ActiveAgentRepository activeAgentRepository;
    private final RollupLevelService rollupLevelService;
    private final TransactionCommonService transactionCommonService;
    private static final ObjectMapper mapper = ObjectMappers.create();
    private static final double NANOSECONDS_PER_MILLISECOND = 1000000.0;

    private final LiveJvmService liveJvmService;

    public AggregateMetricsJsonService (boolean central, List<File> confDirs,
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
    }

    @GET(path = "/backend/error/aggregate-trace-count", permission = "admin:view:aggregateErrorCount")
    String getAggregateErrorCount(@BindRequest AggregateMetricsDataRequest request) throws Exception {
        List<ActiveAgentRepository.TopLevelAgentRollup> topLevelAgents = activeAgentRepository.readActiveTopLevelAgentRollups(request.from(), request.to());
        long errorCount = 0;
        for (ActiveAgentRepository.TopLevelAgentRollup topLevelAgent : topLevelAgents){
            if (topLevelAgent.id().startsWith(request.agentRollupId())){
                TraceRepository.TraceQuery query = ImmutableTraceQuery.builder()
                        .transactionType(request.transactionType())
                        .transactionName(request.transactionName())
                        .from(request.from())
                        .to(request.to())
                        .build();
                errorCount += traceRepository.readErrorCount(topLevelAgent.id(), query);
            }
        }
        return Long.toString(errorCount);
    }

    @GET(path = "/backend/transaction/aggregate-trace-count", permission = "admin:view:aggregateTraceCount")
    String getAggregateSlowTraceCount(@BindRequest AggregateMetricsDataRequest request) throws Exception {
        List<ActiveAgentRepository.TopLevelAgentRollup> topLevelAgents = activeAgentRepository.readActiveTopLevelAgentRollups(request.from(), request.to());
        long slowTraceCount = 0;
        for (ActiveAgentRepository.TopLevelAgentRollup topLevelAgent : topLevelAgents){
            if (topLevelAgent.id().startsWith(request.agentRollupId())){
                TraceRepository.TraceQuery query = ImmutableTraceQuery.builder()
                        .transactionType(request.transactionType())
                        .transactionName(request.transactionName())
                        .from(request.from())
                        .to(request.to())
                        .build();
                slowTraceCount += traceRepository.readSlowCount(topLevelAgent.id(), query);
                boolean includeActiveTraces = shouldIncludeActiveTraces(request);
                if (includeActiveTraces) {
                    slowTraceCount += liveTraceRepository.getMatchingTraceCount(request.transactionType(),
                            request.transactionName());
                }
            }
        }
        return Long.toString(slowTraceCount);
    }

    @GET(path = "/backend/transaction/aggregate-average", permission = "admin:view:aggregateOverview")
    String getAggregateTransactionAverage(@BindRequest AggregateMetricsDataRequest request,
                                          @BindAutoRefresh boolean autoRefresh) throws Exception {
        List<ActiveAgentRepository.TopLevelAgentRollup> topLevelAgents = activeAgentRepository.readActiveTopLevelAgentRollups(request.from(), request.to());
        LiveAggregateRepository.AggregateQuery query = toChartQuery(request, RollupLevelService.DataKind.GENERAL);
        long liveCaptureTime = System.currentTimeMillis();
        List<LiveAggregateRepository.OverviewAggregate> combinedOverviewAggregates = Lists.newArrayList();
        for (ActiveAgentRepository.TopLevelAgentRollup topLevelAgent : topLevelAgents) {
            if (topLevelAgent.id().startsWith(request.agentRollupId())) {
                List<LiveAggregateRepository.OverviewAggregate> overviewAggregates =
                        transactionCommonService.getOverviewAggregates(topLevelAgent.id(), query, autoRefresh);
                if (overviewAggregates.isEmpty() && fallBackToLargestAggregates(query)) {
                    // fall back to largest aggregates in case expiration settings have recently changed
                    query = withLargestRollupLevel(query);
                    overviewAggregates = transactionCommonService.getOverviewAggregates(topLevelAgent.id(),
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
        Map<Long, Double> durations = Maps.newHashMap();
        Map<Long, Long> counts = Maps.newHashMap();
        for (LiveAggregateRepository.OverviewAggregate overviewAggregate : combinedOverviewAggregates){
            if (!durations.containsKey(overviewAggregate.captureTime())){
                durations.put(overviewAggregate.captureTime(), overviewAggregate.totalDurationNanos());
                counts.put(overviewAggregate.captureTime(), overviewAggregate.transactionCount());
            }
            else {
                double oldDuration = durations.get(overviewAggregate.captureTime());
                durations.replace(overviewAggregate.captureTime(), overviewAggregate.totalDurationNanos()+oldDuration);
                long oldCount = counts.get(overviewAggregate.captureTime());
                counts.replace(overviewAggregate.captureTime(), overviewAggregate.transactionCount()+oldCount);
            }
        }
        List<LiveAggregateRepository.OverviewAggregate> averageOverviewAggregates = Lists.newArrayList();
        for (long captureTime : durations.keySet()){
            ImmutableOverviewAggregate.Builder builder = ImmutableOverviewAggregate.builder()
                    .captureTime(captureTime)
                    .totalDurationNanos(durations.get(captureTime))
                    .transactionCount(counts.get(captureTime))
                    .asyncTransactions(false)
                    .mainThreadRootTimers(new ArrayList<AggregateOuterClass.Aggregate.Timer>())
                    .mainThreadStats(AggregateOuterClass.Aggregate.ThreadStats.newBuilder().build());
            averageOverviewAggregates.add(builder.build());
        }
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

    @GET(path="/backend/child-jvm-status", permission="admin:view:childJvmStatus")
    String getAgentJvmStatus(@BindRequest LayoutJsonService.ChildAgentRollupsRequest request,
                             @BindAuthentication HttpSessionManager.Authentication authentication)throws Exception{
        List<ActiveAgentRepository.AgentRollup> childLevelAgents=activeAgentRepository.readActiveChildAgentRollups(request.topLevelId(), request.from(), request.to());
        int countOfLiveJvms=0;
        int countOfDeadJvms=0;
        for(ActiveAgentRepository.AgentRollup childLevelAgent:childLevelAgents){

            boolean status=getJvmStatus(childLevelAgent.id());
            if(status){
                countOfLiveJvms++;
            }else{
                countOfDeadJvms++;
            }

        }
        return "{\"total\":"+childLevelAgents.size()+
                ",\"live\":"+countOfLiveJvms+
                ",\"dead\":"+countOfDeadJvms+"}";
    }

    @GET(path="/backend/aggregate-child-jvm-status", permission="admin:view:aggregateChildJvmStatus")
    String getAgentJvmStatus(@BindRequest SimplifiedAggregateMetricsDataRequest request,
                             @BindAuthentication HttpSessionManager.Authentication authentication)throws Exception{
        List<ActiveAgentRepository.TopLevelAgentRollup> topLevelAgents = activeAgentRepository.readActiveTopLevelAgentRollups(request.from(), request.to());
        int countOfLiveJvms=0;
        int countOfDeadJvms=0;
        int countOfTotalJvms=0;
        for (ActiveAgentRepository.TopLevelAgentRollup topLevelAgent : topLevelAgents){
            if (topLevelAgent.id().startsWith(request.agentRollupId())){
                List<ActiveAgentRepository.AgentRollup> childLevelAgents=activeAgentRepository.readActiveChildAgentRollups(topLevelAgent.id(), request.from(), request.to());
        
                for(ActiveAgentRepository.AgentRollup childLevelAgent:childLevelAgents){
                    countOfTotalJvms++;
                    boolean status=getJvmStatus(childLevelAgent.id());
                    if(status){
                        countOfLiveJvms++;
                    }else{
                        countOfDeadJvms++;
                    }

                }
            }
        }
        return "{\"total\":"+countOfTotalJvms+
                ",\"live\":"+countOfLiveJvms+
                ",\"dead\":"+countOfDeadJvms+"}";
    }

    private boolean shouldIncludeActiveTraces(AggregateMetricsDataRequest request) {

        long currentTimeMillis = System.currentTimeMillis();
        return (request.to() == 0 || request.to() > currentTimeMillis)
                && request.from() < currentTimeMillis;
    }
    private boolean getJvmStatus(String id)throws Exception{
        return liveJvmService.isAvailable(id);
    }

    private LiveAggregateRepository.AggregateQuery toChartQuery(AggregateMetricsDataRequest request, RollupLevelService.DataKind dataKind) throws Exception {
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

    private static List<DataSeries> getDataSeriesForTimerChart(AggregateMetricsDataRequest request,
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

    private static List<DataSeries> getTimerDataSeries(AggregateMetricsDataRequest request,
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

    // calculate top 5 timers
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

    interface RequestBase {
        String agentRollupId();
        String transactionType();
        @Nullable
        String transactionName();
        long from();
        long to();
    }

    @Value.Immutable
    interface AggregateMetricsDataRequest extends RequestBase {}

    interface SimplifiedRequestBase {
        String agentRollupId();
        long from();
        long to();
    }
    
    @Value.Immutable
    interface SimplifiedAggregateMetricsDataRequest extends SimplifiedRequestBase {}

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

    // by using MutableDouble, two operations (get/put) are not required for each increment,
    // instead just a single get is needed (except for first delta)
    @SuppressWarnings("serial")
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
}


