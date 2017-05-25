/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.pcawg.client;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Slf4j
@Configuration
@EnableConfigurationProperties(ApplicationConfig.class)
public class ClientMain implements CommandLineRunner {

  @Autowired
  private ApplicationConfig applicationConfig;

  @Override
  @SneakyThrows
  public void run(String... args) {
    log.info("Args: {}", applicationConfig.toString());

    val importer = Importer.builder()
        .token(applicationConfig.getToken())
        .hdfsEnabled(applicationConfig.isHdfs())
        .outputVcfDir(applicationConfig.getVcf_dir())
        .persistVcfDownloads(applicationConfig.isPersist())
        .bypassMD5Check(applicationConfig.isBypass_md5())
        .outputTsvDir(applicationConfig.getTsv_dir())
        .hdfsHostname(applicationConfig.getHdfs_hostname())
        .hdfsPort(applicationConfig.getHdfs_port())
        .build();
    importer.run();

  }

  public static void main(String... args) {
    new SpringApplicationBuilder(ClientMain.class)
        .addCommandLineProperties(true)
        .run(args);
  }

}
