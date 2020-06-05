package org.apache.hadoop.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

@SuppressWarnings("ALL")
public class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public static <T> T checkNotNull(T ref, String message) {
    if (ref == null) {
      throw new NullPointerException(message);
    }
    return ref;
  }

  public static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }

  public static boolean isNotNullOrEmpty(String str) {
    return !isNullOrEmpty(str);
  }

  public static String getProperty(Map<String, ?> properties, String propertyName, String defaultValue) {
    String answer = (String)properties.get(propertyName);
    if (!Utils.isNullOrEmpty(answer)) {
      return answer;
    }

    return getSystemPropertyOrEnvVar(propertyName, defaultValue);
  }

  public static String getProperty(Map<String, ?> properties, String propertyName) {
    return getProperty(properties, propertyName, null);
  }

  public static String getProperty(Properties properties, String propertyName, String defaultValue) {
    String answer = (String)properties.get(propertyName);
    if (!Utils.isNullOrEmpty(answer)) {
      return answer;
    }

    return getSystemPropertyOrEnvVar(propertyName, defaultValue);
  }

  public static String getProperty(Properties properties, String propertyName) {
    return getProperty(properties, propertyName, null);
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName, String envVarName, String defaultValue) {
    String answer = System.getProperty(systemPropertyName);
    if (isNotNullOrEmpty(answer)) {
      return answer;
    }

    answer = System.getenv(envVarName);
    if (isNotNullOrEmpty(answer)) {
      return answer;
    }

    return defaultValue;
  }

  public static String convertSystemPropertyNameToEnvVar(String systemPropertyName) {
    return systemPropertyName.toUpperCase().replaceAll("[.-]", "_");
  }

  public static String getEnvVar(String envVarName, String defaultValue) {
    String answer = System.getenv(envVarName);
    return isNotNullOrEmpty(answer) ? answer : defaultValue;
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName, String defaultValue) {
    return getSystemPropertyOrEnvVar(systemPropertyName,
        convertSystemPropertyNameToEnvVar(systemPropertyName), defaultValue);
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName) {
    return getSystemPropertyOrEnvVar(systemPropertyName, (String) null);
  }

  public static Boolean getSystemPropertyOrEnvVar(String systemPropertyName, Boolean defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, defaultValue.toString());
    return Boolean.parseBoolean(result);
  }

  public static int getSystemPropertyOrEnvVar(String systemPropertyName, int defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, Integer.toString(defaultValue));
    return Integer.parseInt(result);
  }

  public static long getSystemPropertyOrEnvVar(String systemPropertyName, long defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, Long.toString(defaultValue));
    return Long.parseLong(result);
  }

  public static Float getSystemPropertyOrEnvVar(String systemPropertyName, float defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, Float.toString(defaultValue));
    return Float.parseFloat(result);
  }

  public static String join(final Object[] array) {
    return join(array, ',');
  }

  public static String join(final Object[] array, final char separator) {
    if (array == null) {
      return null;
    }
    if (array.length == 0) {
      return "";
    }
    final StringBuilder buf = new StringBuilder();
    for (int i = 0; i < array.length; i++) {
      if (i > 0) {
        buf.append(separator);
      }
      if (array[i] != null) {
        buf.append(array[i]);
      }
    }
    return buf.toString();
  }
}
