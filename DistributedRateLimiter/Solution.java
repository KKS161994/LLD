package DistributedRateLimiter;

import java.util.*;

import DistributedRateLimiter.Solution.Application.ClientType;
import DistributedRateLimiter.Solution.RequestSoFar;

public class Solution {

    static class Quota {
        int val;
        int bucket;

        Quota(int val, int bucket) {
            this.val = val;
            this.bucket = bucket;
        }
    }

    static class Default {
        static int SERVICE_REQUEST_COUNT = 100;
        static int SERVICE_INTERVAL_BUCKET = 2;
        static int USER_REQUEST_COUNT = 100;
        static int USER_INTERVAL_BUCKET = 2;
        static int IP_REQUEST_COUNT = 100;
        static int IP_INTERVAL_BUCKET = 2;

    }

    static class Application {
        enum ClientType {
            SERVICE,
            USER,
            IP,
        }

        Map<ClientType, Quota> clientQuotas;
        String id;

        Application(String id) {
            this.id = id;
            clientQuotas = new HashMap<>();
            clientQuotas.put(ClientType.USER, get(ClientType.USER));
            clientQuotas.put(ClientType.SERVICE, get(ClientType.SERVICE));
            clientQuotas.put(ClientType.IP, get(ClientType.IP));

        }

        void updateQuota(ClientType clientType, int val, int bucketSize) {

            Quota quota = clientQuotas.get(clientType);
            quota.val = val;
            quota.bucket = bucketSize;
        }

        Quota get(ClientType clientType) {
            switch (clientType) {
                case ClientType.SERVICE:
                    return new Quota(
                            Default.SERVICE_REQUEST_COUNT,
                            Default.SERVICE_INTERVAL_BUCKET);

                case ClientType.IP:
                    return new Quota(
                            Default.IP_REQUEST_COUNT,
                            Default.IP_INTERVAL_BUCKET);
                case ClientType.USER:
                    return new Quota(
                            Default.USER_REQUEST_COUNT,
                            Default.USER_INTERVAL_BUCKET);

                default:
                    break;
            }
            return null;
        }

    }

    static class ApplicationService {
        private Map<String, Application> idToAppMap;
        private static ApplicationService INSTANCE;

        private ApplicationService() {
            idToAppMap = new HashMap<>();
        }

        static ApplicationService getInstance() {
            if (INSTANCE == null) {
                INSTANCE = new ApplicationService();
            }

            return INSTANCE;
        }

        boolean createApplication(Application application) {
            if (idToAppMap.putIfAbsent(application.id, application) != null)
                return false;
            return true;
        }

        boolean updateQuota(String id, ClientType clientType,
                int val, int bucket) {
            Application application = idToAppMap.get(id);
            if (application == null)
                return false;
            Quota quota = application.get(clientType);
            quota.val = val;
            quota.bucket = bucket;
            return true;
        }

        Application geApplication(String id) {
            return idToAppMap.get(id);
        }

    }

    static class RateLimiterService {
        ApplicationService applicationService;
        RateLimiter rateLimiter;

        RateLimiterService() {
            this.applicationService = ApplicationService.getInstance();
            this.rateLimiter = RateLimiter.getInstance();
        }

        boolean allow(String applicationId, String serviceId,
                String user, String ip,
                int timeStamp) {
            Application application = applicationService.geApplication(applicationId);
            // Allow unregistered application for now
            if (application == null)
                return true;

            return rateLimiter.shouldAllow(
                    applicationId, serviceId, application.clientQuotas.get(ClientType.SERVICE), ClientType.SERVICE,
                    timeStamp)
                    && rateLimiter.shouldAllow(
                            applicationId, ip, application.clientQuotas.get(ClientType.IP), ClientType.IP,
                            timeStamp)
                    && rateLimiter.shouldAllow(
                            applicationId, user, application.clientQuotas.get(ClientType.USER), ClientType.USER,
                            timeStamp);

        }
    }

    static class RequestSoFar {
        int requestsCount = 0;
        int currentBucket = 0;

    }

    static class Client {
        int id;
    }

    static class ClientsRequests {
        Map<String, RequestSoFar> clientsRequest;

        ClientsRequests() {
            this.clientsRequest = new HashMap<>();
        }

        boolean configRequest(String clientId) {
            if (clientsRequest.putIfAbsent(clientId, new RequestSoFar()) == null) {
                return true;
            }
            return false;
        }

        RequestSoFar requestSoFar(String clientId) {
            return clientsRequest.get(clientId);
        }
    }

    static class ApplicationRequests {
        private Map<ClientType, ClientsRequests> clientsForClientType;

        ApplicationRequests() {
            this.clientsForClientType = new HashMap<>();
        }

        ClientsRequests getClients(ClientType clientType) {
            clientsForClientType.putIfAbsent(clientType, new ClientsRequests());
            return clientsForClientType.get(clientType);
        }
    }

    static class RateLimiter {

        Map<String, ApplicationRequests> applicationRequestsMap;
        static RateLimiter INSTANCE;

        private RateLimiter() {
            applicationRequestsMap = new HashMap<>();
        }

        public void configureApplication(String applicationID, ClientType clientType, String clientId) {
            applicationRequestsMap.putIfAbsent(applicationID, new ApplicationRequests());
            ApplicationRequests applicationRequest = applicationRequestsMap.get(applicationID);
            ClientsRequests clientsRequests = applicationRequest.getClients(clientType);
            clientsRequests.configRequest(clientId);

        }

        static RateLimiter getInstance() {
            if (INSTANCE == null) {
                INSTANCE = new RateLimiter();
            }
            return INSTANCE;
        }

        boolean shouldAllow(String applicaitonId, String clientId, Quota quota,
                ClientType clientType, int timeStamp) {

            if (!applicationRequestsMap.containsKey(applicaitonId))
                return true;

            ApplicationRequests applicationRequests = applicationRequestsMap.get(applicaitonId);
            ClientsRequests clientsRequests = applicationRequests.getClients(clientType);
            RequestSoFar requestSoFar = clientsRequests.requestSoFar(clientId);
            int currentBucket = timeStamp / quota.bucket;

            if (requestSoFar == null) {
                clientsRequests.configRequest(clientId);
                requestSoFar = clientsRequests.requestSoFar(clientId);
                requestSoFar.requestsCount = 1;
                requestSoFar.currentBucket = currentBucket;
                return true;
            }

            if (currentBucket < requestSoFar.currentBucket)
                return false;
            if (currentBucket > requestSoFar.currentBucket) {
                requestSoFar.currentBucket = currentBucket;
                requestSoFar.requestsCount = 1;
                return true;
            }
            if (requestSoFar.requestsCount < quota.val) {
                requestSoFar.requestsCount += 1;
                return true;
            }
            return false;

        }
    }
}
