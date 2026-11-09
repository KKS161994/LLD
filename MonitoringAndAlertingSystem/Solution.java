package MonitoringAndAlertingSystem;

import java.util.*;

public class Solution {

    enum Operator {
        GREATER,
        LESS_THAN
    }

    enum MetricStatus {
        HEALTHY,
        FIRING
    }

    static enum RulePriority {
        WARNING,
        HIGH,
        CRITICAL
    }

    static class IndividualRule {
        int threshold_value;
        Operator operator;
        MetricStatus metricStatus = MetricStatus.HEALTHY;
        String emailTo;
        Map<RulePriority, Integer> prioritMap = new HashMap<>();
        RulePriority priority;

        IndividualRule() {
            prioritMap.put(RulePriority.WARNING, 1);
            prioritMap.put(RulePriority.HIGH, 2);
            prioritMap.put(RulePriority.CRITICAL, 3);
        }

        public int getPriority() {
            return prioritMap.get(priority);
        }

        boolean evaluate(int value) {
            switch (operator) {
                case Operator.GREATER:
                    if (value > threshold_value) {
                        if (metricStatus.equals(MetricStatus.HEALTHY)) {
                            metricStatus = MetricStatus.FIRING;
                            return true;
                        }
                        return false;
                    }

                    break;

                case Operator.LESS_THAN:
                    if (value < threshold_value) {
                        if (metricStatus.equals(MetricStatus.HEALTHY)) {
                            metricStatus = MetricStatus.FIRING;
                            return true;
                        }
                        return false;
                    }
                    break;
            }
            metricStatus = MetricStatus.HEALTHY;
            return false;
        }
    }

    static class AlertRule {
        String metric;
        List<IndividualRule> rules;

        AlertRule(String metric) {
            this.metric = metric;
            this.rules = new ArrayList<>();
        }

        void addRule(IndividualRule rule) {
            this.rules.add(rule);
        }

        IndividualRule evaluateRules(int value) {
            IndividualRule voilatRule = null;
            for (IndividualRule rule : rules) {
                boolean ruleStateChanged = rule.evaluate(value);
                if (!ruleStateChanged)
                    continue;

                if (voilatRule == null) {
                    voilatRule = rule;
                }

                if (voilatRule.getPriority() < rule.getPriority() ||
                        ((voilatRule.getPriority() == rule.getPriority()) && (Math
                                .abs(value - rule.threshold_value) < Math.abs(value - voilatRule.threshold_value))))
                    voilatRule = rule;

            }

            return voilatRule;
        }
    }

    static class Application {
        enum Status {
            REGISTERED,
            DISABLED,
        }

        private String id;
        private Status status;
        Map<String, AlertRule> metricToRules;

        Application(String id) {
            this.id = id;
            status = Status.REGISTERED;
            metricToRules = new HashMap<>();
        }

        public void removeRegistration() {
            status = Status.DISABLED;
        }

        public void addRule(String metric, IndividualRule rule) {
            metricToRules.putIfAbsent(metric, new AlertRule(metric));
            metricToRules.get(metric).addRule(rule);
        }

        public AlertRule getAlertRule(String metric) {
            return metricToRules.get(metric);
        }

        public Status getRegistrationStatus() {
            return this.status;
        }
    }

    static class ApplicationService {
        Map<String, Application> idToApplicationMap;
        static ApplicationService INSTANCE;

        public static ApplicationService getInstance() {
            if (INSTANCE == null) {
                INSTANCE = new ApplicationService();
            }
            return INSTANCE;
        }

        private ApplicationService() {
            idToApplicationMap = new HashMap<>();
        }

        public Application getApplication(String serviceId) {
            return idToApplicationMap.get(serviceId);
        }

        public boolean registerApplication(String serviceId) {
            idToApplicationMap.putIfAbsent(serviceId, new Application(serviceId));
            return true;
        }

        public boolean disableRegistration(String serviceId) {
            idToApplicationMap.putIfAbsent(serviceId, new Application(serviceId));
            idToApplicationMap.get(serviceId).removeRegistration();
            return true;
        }

    }

    static class MonitoringMetrics {
        private ApplicationService applicationService;

        private AlertingService alertingService;

        MonitoringMetrics() {
            this.applicationService = ApplicationService.getInstance();
        }

        void ingestMetrics(String serviceId, String metricId, int value, long timestamp) {
            synchronized (serviceId) {
                Application application = this.applicationService.getApplication(serviceId);
                if (application.getRegistrationStatus()
                        .equals(Application.Status.DISABLED))
                    return;
                AlertRule alertRule = application.getAlertRule(metricId);
                IndividualRule voilatedRule = alertRule.evaluateRules(value);
                if (voilatedRule == null)
                    return;

                Alert.triggerError(

                        serviceId,
                        metricId,
                        voilatedRule.emailTo,
                        formError(serviceId, metricId, voilatedRule));
            }
        }

        String formError(String serviceId, String metricId, IndividualRule rule) {
            return "Service " + serviceId + " metric " + metricId +
                    "is " + (rule.operator == Operator.GREATER ? ">" : "<") + " " + rule.threshold_value;
        }
    }

    static class Alert {
        String owner;
        String metric;
        String serviceId;
        String error;
        static void triggerError(String serviceId, String owner, String metric, String error) {
            // trigger notification to owner
            AlertingService.getInstance()
                    .alertOwner(metric, serviceId, error, owner);
        }
    }

    static class AlertingService {
        public static AlertingService INSTANCE;

        public static AlertingService getInstance() {
            synchronized(INSTANCE){
                INSTANCE = new AlertingService();
            }
            return INSTANCE;
        }

        public void alertOwner(String metric, String serviceId, String error, String owner) {
                
        }
    }
}
