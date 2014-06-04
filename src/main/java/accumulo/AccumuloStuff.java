/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package accumulo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class AccumuloStuff {
  private static final Logger log = LoggerFactory.getLogger(AccumuloStuff.class);

  private static void setCoreSite(MiniAccumuloClusterImpl cluster) throws Exception {
    File csFile = new File(cluster.getConfig().getConfDir(), "core-site.xml");
    if (csFile.exists())
      throw new RuntimeException(csFile + " already exist");

    Configuration coreSite = new Configuration(false);
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(cluster.getConfig().getConfDir(), "core-site.xml")));
    coreSite.writeXml(out);
    out.close();
  }

  public static void main(String[] args) throws Exception {
    File tmp = new File(System.getProperty("user.dir") + "/target/mac-test");
    if (tmp.exists()) {
      FileUtils.deleteDirectory(tmp);
    }
    tmp.mkdirs();
    String passwd = "password";

    MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(tmp, passwd);
    cfg.setNumTservers(1);
//    cfg.useMiniDFS(true);

    final MiniAccumuloClusterImpl cluster = cfg.build();
    setCoreSite(cluster);
    cluster.start();

    ExecutorService svc = Executors.newFixedThreadPool(2);

    try {
      Connector conn = cluster.getConnector("root", passwd);
      String table = "table";
      conn.tableOperations().create(table);
     
      final BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      final AtomicBoolean flushed = new AtomicBoolean(false);

      Runnable writer = new Runnable() {
        @Override
        public void run() {
          try {
            Mutation m = new Mutation("row");
            m.put("colf", "colq", "value");
            bw.addMutation(m);
            bw.flush();
            flushed.set(true);
          } catch (Exception e) {
            log.error("Got exception trying to flush mutation", e);
          }

          log.info("Exiting batchwriter thread");
        }
      };

      Runnable restarter = new Runnable() {
        @Override
        public void run() {
          try {
            for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
              cluster.killProcess(ServerType.TABLET_SERVER, proc);
            }
            cluster.exec(TabletServer.class);
          } catch (Exception e) {
            log.error("Caught exception restarting tabletserver", e);
          }
          log.info("Exiting restart thread");
        }
      };

      svc.execute(writer);
      svc.execute(restarter);

      log.info("Waiting for shutdown");
      svc.shutdown();
      if (!svc.awaitTermination(120, TimeUnit.SECONDS)) {
        log.info("Timeout on shutdown exceeded");
        svc.shutdownNow();
      } else {
        log.info("Cleanly shutdown");
        log.info("Threadpool is terminated? " + svc.isTerminated());
      }

      if (flushed.get()) {
        log.info("****** BatchWriter was flushed *********");
      } else {
        log.info("****** BatchWriter was NOT flushed *********");
      }

      bw.close();

      log.info("Got record {}", Iterables.getOnlyElement(conn.createScanner(table, Authorizations.EMPTY)));
    } finally {
      cluster.stop();
    }
  }
}
