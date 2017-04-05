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
package org.icgc.dcc.pcawg.client.download;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.hash.Hashing;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.portal.PortalMetadata;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.nio.file.Files.copy;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_API;

@Value
@Slf4j
public class Storage {


  private final boolean persist;

  private final Path outputDir;

  private final long currentTime;

  private final Path tempFile;

  private final boolean bypassMD5Check;

  private final String token;

  private static Path createTempFile(Path outputDir){
    val filename = "tmp." + System.currentTimeMillis() + ".vcf.gz";
    val path = outputDir.resolve(filename);
    path.toFile().deleteOnExit();
    return path;
  }

  public static Storage newStorage(final boolean persist, String outputDirName, final boolean bypassMD5Check, String token){
    return new Storage(persist, outputDirName, bypassMD5Check, token);
  }

  private Storage(final boolean persist, @NonNull final String outputDirName, final boolean bypassMD5Check, String token) {
    this.bypassMD5Check = bypassMD5Check;
    this.persist = persist;
    this.token = token;
    this.outputDir = Paths.get(outputDirName).toAbsolutePath();
    initDir(outputDir);
    this.currentTime = System.currentTimeMillis();
    this.tempFile = createTempFile(outputDir);
  }

  @SneakyThrows
  public static void initDir(@NonNull final Path dir) {
    val dirDoesNotExist = !Files.exists(dir);
    if (dirDoesNotExist) {
      Files.createDirectories(dir);
    }
  }

  private void checkForParentDir(@NonNull Path file) {
    Path absoluteFile = file;
    if (!file.isAbsolute()) {
      absoluteFile = file.toAbsolutePath();
    }
    checkState(absoluteFile.startsWith(outputDir),
        "The file [%s] must have the parent directory [%s] in its path",
        absoluteFile, outputDir);
  }

  // Used for subdirectories inside outputDir
  public static void initParentDir(@NonNull Path file) {
    val parentDir = file.getParent();
    initDir(parentDir);
  }

  // Download file regardless of persist mode
  @SneakyThrows
  private File downloadFileByObjectId(@NonNull final String objectId, @NonNull final String filename) {
    val objectUrl = getObjectUrl(STORAGE_API,objectId);
    val output = Paths.get(filename);

    @Cleanup
    val input = objectUrl.openStream();
    copy(input, output, REPLACE_EXISTING);

    return output.toFile();
  }

  @SneakyThrows
  public static File downloadFileByURL(@NonNull final String urlString, @NonNull final String outputFilename) {
    val objectUrl = new URL(urlString);
    val output = Paths.get(outputFilename);

    @Cleanup
    val input = objectUrl.openStream();
    copy(input, output, REPLACE_EXISTING);

    return output.toFile();
  }

  @SneakyThrows
  public File downloadFile(@NonNull final PortalMetadata portalMetadata) {
    val objectId = portalMetadata.getObjectId();
    val expectedMD5Sum = portalMetadata.getFileMd5sum();
    val relativeFilename = portalMetadata.getPortalFilename().getFilename();
    val relativeFile = Paths.get(relativeFilename);
    val absFile = outputDir.resolve(relativeFile).toAbsolutePath();
    val absFilename = absFile.toString();
    checkForParentDir(absFile);
    initParentDir(absFile);
    val fileExists = Files.exists(absFile);
    val md5Match = bypassMD5Check || (fileExists && calcMd5Sum(absFile).equals(expectedMD5Sum)); // Short circuit
    if (persist) {
      if (md5Match) {
        log.info("File [{}] already exists and matches checksum. Skipping download.", absFile);
        return absFile.toFile();
      } else {
        return downloadFileByObjectId(objectId, absFilename);
      }
    } else {
      return downloadFileByObjectId(objectId, tempFile.toAbsolutePath().toString());
    }
  }

  private static String calcMd5Sum(@NonNull final Path file) throws IOException {
    checkState(file.toFile().isFile(), "The input path [%s] is not a file", file);
    val bytes = Files.readAllBytes(file);
    return Hashing.md5()
        .newHasher()
        .putBytes(bytes)
        .hash()
        .toString();
  }


  @SneakyThrows
  public URL getObjectUrl(@NonNull final String api, @NonNull final String objectId) {
    val storageUrl = new URL(api + "/download/" + objectId + "?offset=0&length=-1&external=true");
    val connection = (HttpURLConnection) storageUrl.openConnection();
    connection.setRequestProperty(AUTHORIZATION, "Bearer " + token);
    val object = readObject(connection);
    return getUrl(object);
  }


  @SneakyThrows
  private static URL getUrl(JsonNode object) {
    return new URL(object.get("parts").get(0).get("url").textValue());
  }

  @SneakyThrows
  private static JsonNode readObject(@NonNull final HttpURLConnection connection) {
    return DEFAULT.readTree(connection.getInputStream());
  }

}
