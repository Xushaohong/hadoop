package org.apache.hadoop.security.sasl;

import com.tencent.tdw.security.utils.StringUtils;
import org.apache.hadoop.security.Utils;

import java.util.Collection;

public enum QOP {
    AUTH_CONF("auth-conf", "privacy", 4), AUTH_INIT("auth-int", "integrity", 2), AUTH("auth",
        "auth", 1);
    private String name;
    private String desc;
    private int level;

    QOP(String name, String desc, int level) {
      this.name = name;
      this.desc = desc;
      this.level = level;
    }


    public String getName() {
      return name;
    }

    public String getDesc() {
      return desc;
    }

    public int getLevel() {
      return level;
    }

    public static QOP getByName(String name) {
      for (QOP qop : values()) {
        if (qop.name.equalsIgnoreCase(name)) {
          return qop;
        }
      }
      return null;
    }

    public static QOP getByLevel(int level) {
      for (QOP qop : values()) {
        if (qop.level == level) {
          return qop;
        }
      }
      return null;
    }

    public static QOP[] parseQop(String qopProp) {
      if (Utils.isNullOrEmpty(qopProp)) {
        return new QOP[]{QOP.AUTH};
      }
      String[] qopItems = StringUtils.split(qopProp);
      QOP[] qops = new QOP[qopItems.length];
      for (int i = 0; i < qopItems.length; i++) {
        qops[i] = QOP.getByName(qopItems[i]);
        if (qops[i] == null) {
          qops[i] = QOP.AUTH;
        }
      }
      return qops;
    }

    public static QOP getHighestQop(Collection<QOP> qops) {
      int level = QOP.AUTH.level;
      for (QOP qop : qops) {
        if (qop.level > level) {
          level = qop.level;
        }
      }
      return QOP.getByLevel(level);
    }
  }