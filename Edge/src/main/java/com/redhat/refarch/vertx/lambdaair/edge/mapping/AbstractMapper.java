package com.redhat.refarch.vertx.lambdaair.edge.mapping;

import io.vertx.core.http.HttpServerRequest;

import java.util.logging.Logger;

public abstract class AbstractMapper implements Mapper {
    private static Logger logger = Logger.getLogger(AbstractMapper.class.getName());

    public String getServiceName(HttpServerRequest request) {
        String fullPath = request.path();
        logger.fine("Path " + fullPath);

        if (fullPath == null || fullPath.length() == 0) {
            return null;
        } else {
            int separatorIndex = fullPath.indexOf("/", 1);
            logger.fine("Service name separator found at position " + separatorIndex);
            if (separatorIndex > 0) {
                String serviceName = fullPath.substring(1, separatorIndex);
                logger.fine("Service name determined as " + serviceName);
                return serviceName;
            } else {
                return null;
            }
        }
    }

    public String getRoutedAddress(HttpServerRequest request, String serviceAddress) {
        String fullPath = request.path();
        if (fullPath == null || fullPath.length() == 0) {
            return null;
        } else {
            int separatorIndex = fullPath.indexOf("/", 1);
            if (separatorIndex > 0) {
                String servicePath = fullPath.substring(separatorIndex);
                String url = serviceAddress + servicePath;

                String query = request.query();
                logger.fine("Query " + query);
                if (query != null) {
                    url += "?" + query;
                }
                logger.fine("Returning full URL " + url);
                return url;
            } else {
                return null;
            }
        }
    }
}
