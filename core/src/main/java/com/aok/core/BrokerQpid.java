package com.aok.core;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.SystemConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a sample qpid server using memory-store.
 */
public class BrokerQpid {
    private static void startBroker(Map<String,Object> attributes) throws Exception
    {
        SystemLauncher systemLauncher = new SystemLauncher(new SystemLauncherListener.DefaultSystemLauncherListener(),
                new SystemLauncherListener.DefaultSystemLauncherListener()
                {
                    @Override
                    public void onShutdown(final int exitCode)
                    {
                        if (exitCode != 0)
                        {
                            shutdown(exitCode);
                        }
                    }
                });
        systemLauncher.startup(attributes);
    }

    private static void shutdown(int exitCode) {
        System.exit(exitCode);
    }

    public static void main(String[] args) throws Exception {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put("storePath", BrokerQpid.class.getClassLoader().getResource("config.json").getPath());
        attributes.put("qpid.work_dir", BrokerQpid.class.getClassLoader().getResource(".").getPath());
        attributes.put(SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE);
        startBroker(attributes);
    }
}
