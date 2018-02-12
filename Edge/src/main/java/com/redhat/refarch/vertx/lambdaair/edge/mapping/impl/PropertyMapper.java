package com.redhat.refarch.vertx.lambdaair.edge.mapping.impl;

import com.redhat.refarch.vertx.lambdaair.edge.mapping.AbstractMapper;
import io.vertx.core.http.HttpServerRequest;

import java.util.logging.Logger;

public class PropertyMapper extends AbstractMapper {
    private static final PropertyMapper INSTANCE = new PropertyMapper();
    private static final String HOST_KEY_PREFIX = "edge.proxy.";
    private static final String HOST_KEY_SUFFIX = ".address";
    private static Logger logger = Logger.getLogger(PropertyMapper.class.getName());

    private PropertyMapper() {
    }

    public static PropertyMapper getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean initialize() {
        return true;
    }

    @Override
    public String getHostAddress(HttpServerRequest request, String hostAddress) {
        String serviceName = getServiceName(request);
        String host = getHost(serviceName);
        if (host == null) {
            logger.fine(serviceName + " not mapped through properties");
            return hostAddress;
        } else {
            logger.fine("Mapped to host " + host);
            return getRoutedAddress(request, host);
        }
    }

    private String getHost(String context) {
        return System.getProperty(HOST_KEY_PREFIX + context + HOST_KEY_SUFFIX);
    }
}
