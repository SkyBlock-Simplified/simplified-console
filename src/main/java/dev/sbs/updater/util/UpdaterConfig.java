package dev.sbs.updater.util;

import dev.sbs.api.data.sql.SqlConfig;

import java.io.File;

public class UpdaterConfig extends SqlConfig {

    public UpdaterConfig(File configDir, String fileName, String... header) {
        super(configDir, fileName, header);
    }

}
