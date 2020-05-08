package org.apache.hadoop.crypto.key.kms.server;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.crypto.key.ZooKeeperKeyStoreProvider;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program is the CLI utility for the KMS facilities in Hadoop.
 */
public class KMSShell extends Configured implements Tool {
  private static final String USAGE_PREFIX = "Usage: kms kms\n";
  private static final String COMMANDS =
      "   [-help]\n" +
      "   [" + FormatKeyStoreCommand.USAGE + "]\n" +
      "   [" + ImportKeyStoreCommand.USAGE + "]\n";

  private Command command = null;

  // allows stdout to be captured if necessary
  private PrintStream out = System.out;
  // allows stderr to be captured if necessary
  private PrintStream err = System.err;

  /**
   * Primary entry point for the KMSShell; called via main().
   *
   * @param args Command line arguments.
   * @return 0 on success and 1 on failure.  This value is passed back to
   * the unix shell, so we must follow shell return code conventions:
   * the return code is an unsigned character, and 0 means success, and
   * small positive integers mean failure.
   */
  @Override
  public int run(String[] args) throws Exception {
    int exitCode = 0;
    try {
      exitCode = init(args);
      if (exitCode != 0) {
        return exitCode;
      }
      if (command.validate()) {
        command.execute();
      } else {
        exitCode = 1;
      }
    } catch (Exception e) {
      e.printStackTrace(err);
      return 1;
    }
    return exitCode;
  }

  /**
   * Parse the command line arguments and initialize the data.
   * <pre>
   * % bin/kms kms -formatKeyStore [-force] [-nonInteractive]
   * </pre>
   * @param args Command line arguments.
   * @return 0 on success, 1 on failure.
   * @throws IOException
   */
  private int init(String[] args) throws IOException {
    if (args.length <= 0) {
      printKMSShellUsage();
      return 1;
    }

    if ("-formatKeyStore".equals(args[0])) {
      command = new FormatKeyStoreCommand(args);
    } else if ("-importKeyStore".equals(args[0])) {
      command = new ImportKeyStoreCommand(args);
    } else if ("-help".equals(args[0])){
      printKMSShellUsage();
      return 1;
    } else {
      err.println("Bad argument: " + args[0]);
      return 1;
    }

    return 0;
  }

  private void printKMSShellUsage() {
    out.println(USAGE_PREFIX + COMMANDS);
    if (command != null) {
      out.println(command.getUsage());
    } else {
      out.println("=========================================================");
      out.println(FormatKeyStoreCommand.USAGE + ":\n\n"
          + FormatKeyStoreCommand.DESC);
      out.println("=========================================================");
      out.println(ImportKeyStoreCommand.USAGE + ":\n\n"
          + ImportKeyStoreCommand.DESC);
    }
  }

  private abstract class Command {
    public boolean validate() {
      return true;
    }

    public abstract void execute() throws Exception;

    public abstract String getUsage();
  }

  private class FormatKeyStoreCommand extends Command {
    public static final String USAGE =
        "-formatKeyStore [-force] [-nonInteractive]";
    public static final String DESC =
        "The format subcommand formats the storage directory for the kms keys "
        + "in the zk cluster.";

    private final String[] args;
    private boolean force = false;
    private boolean interactive = true;

    FormatKeyStoreCommand(String[] args) {
      this.args = args;
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }

    @Override
    public boolean validate() {
      // check arguments
      for (int i = 1; i < args.length; i++) {
        if ("-force".equals(args[i])) {
          force = true;
        } else if ("-nonInteractive".equals(args[i])) {
          interactive = false;
        } else {
          out.println("Bad argument: " + args[i]);
          return false;
        }
      }
      return true;
    }

    @Override
    public void execute() throws IOException {
      // create a KeyProvider instance and execute "formatZK" command
      Configuration kmsConf = getConf();
      String providerString = kmsConf.get(KMSConfiguration.KEY_PROVIDER_URI);
      if (providerString == null) {
        throw new IllegalStateException("No KeyProvider has been defined");
      }

      try {
        URI uri = new URI(providerString);
        if (!uri.getScheme().equals(ZooKeeperKeyStoreProvider.SCHEME_NAME)) {
          throw new IllegalStateException("scheme: " + uri.getScheme() +
              " is not ZooKeeperKeyStoreProvider scheme!");
        }
        ZooKeeperKeyStoreProvider.formatKeyStore(uri, kmsConf,
            force, interactive);
      } catch (URISyntaxException eu) {
        throw new IOException("Ilegal URI: " + providerString, eu);
      } catch (InterruptedException ei) {
        throw new IOException("Interrupted while exec format command", ei);
      }
    }
  }

  private class ImportKeyStoreCommand extends Command {
    public static final String USAGE =
        "-importKeyStore <path>";
    public static final String DESC =
        "The import subcommand import the key storage content from the local "
        + "file to zk cluster.";

    private final String[] args;
    private String path;

    ImportKeyStoreCommand(String[] args) {
      this.args = args;
    }

    @Override
    public String getUsage() {
      return USAGE + ":\n\n" + DESC;
    }

    @Override
    public boolean validate() {
      // check arguments
      int maxArgs = 2;
      path = args[1];
      if (args.length > maxArgs) {
        out.println("Bad argument: " + args[maxArgs]);
        return false;
      }

      return true;
    }

    @Override
    public void execute() throws IOException {
     // create a KeyProvider instance and execute "importKeyStore" command
      Configuration kmsConf = getConf();
      String providerString = kmsConf.get(KMSConfiguration.KEY_PROVIDER_URI);
      if (providerString == null) {
        throw new IllegalStateException("No KeyProvider has been defined");
      }

      try {
        URI uri = new URI(providerString);
        if (!uri.getScheme().equals(ZooKeeperKeyStoreProvider.SCHEME_NAME)) {
          throw new IllegalStateException("scheme: " + uri.getScheme() +
              " is not ZooKeeperKeyStoreProvider scheme!");
        }
        ZooKeeperKeyStoreProvider.importKeyStore(uri, kmsConf, path);
      } catch (URISyntaxException eu) {
        throw new IOException("Ilegal URI: " + providerString, eu);
      }
    }
  }

  /**
   * main() entry point for the KeyShell.  While strictly speaking the
   * return is void, it will System.exit() with a return code: 0 is for
   * success and 1 for failure.
   *
   * @param args Command line arguments.
   * @throws Exception if any exceptions arose.
   */
  public static void main(String[] args) throws Exception {
    Configuration kmsConf = KMSConfiguration.getKMSConf();
    int res = ToolRunner.run(kmsConf, new KMSShell(), args);
    System.exit(res);
  }
}
