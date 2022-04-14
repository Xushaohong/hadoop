package com.tencent.mdfs.config;

import com.qq.cloud.taf.support.om.CommandHandler;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * create:lichunxiao
 * Date:2019-03-14
 */
public class VnnCommandHandler implements CommandHandler {

    public static final String CMD_UPDATE_CONFIG = "update_config";
    public static final String CMD_SET_LOG_LEVEL = "set_log_level";

    private static final Logger LOGGER = LogManager.getLogger(VnnCommandHandler.class);

    @Override
    public void handle(String cmd, String json) {

        VnnLogUtil.log("ConfigLoader handle " + cmd + " " + json);

        if(StringUtils.isEmpty(cmd)){
            LOGGER.error("cmd is null ");
            return;
        }

        switch (cmd) {
            case CMD_UPDATE_CONFIG:
                VnnConfigUtil.loadMainPropertiesConfig();
                VnnConfigUtil.refresh();
                break;
            default:
                VnnLogUtil.err("invalid cmd " + cmd + " " + json);
        }
    }
}
