package com.redhat.refarch.vertx.lambdaair.edge.mapping;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PropertyMapper extends AbstractVerticle {
    private static final String HOST_KEY_PREFIX = "edge.proxy.";
    private static final String HOST_KEY_SUFFIX = ".address";
    private static Logger logger = Logger.getLogger(PropertyMapper.class.getName());


    @Override
    public void start(Future<Void> startFuture) {
        vertx.eventBus().consumer(PropertyMapper.class.getName(), PropertyMapper::mapAddress);
        startFuture.complete();
    }

    private static void mapAddress(Message<JsonObject> message) {
        JsonObject jsonObject = message.body();
        String path = jsonObject.getString("path");
        String serviceName = getServiceName(path);
        String hostUrl = getHost(serviceName);
        if (hostUrl == null) {
            logger.fine(serviceName + " not mapped through properties");
        } else {
            try {
                URL url = new URL(hostUrl);
                jsonObject.put("scheme", url.getProtocol());
                jsonObject.put("host", url.getHost() + ":" + url.getPort());
                logger.fine("Mapped to host " + jsonObject.getString("host"));
                jsonObject.put("path", getProxyPath(path));

            } catch (MalformedURLException e) {
                logger.log(Level.SEVERE, "Could not parse mapped URL " + hostUrl, e);
            }
        }
        message.reply(jsonObject);
    }

    private static String getHost(String context) {
        return System.getProperty(HOST_KEY_PREFIX + context + HOST_KEY_SUFFIX);
    }

    private static String getServiceName(String fullPath) {
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

    private static String getProxyPath(String fullPath) {
        if (fullPath == null || fullPath.length() == 0) {
            return null;
        } else {
            int separatorIndex = fullPath.indexOf("/", 1);
            if (separatorIndex > 0) {
                return fullPath.substring(separatorIndex);
            } else {
                return null;
            }
        }
    }
}
