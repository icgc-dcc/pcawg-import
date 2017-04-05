/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.pcawg.client.model.portal;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.download.PortalFiles;

import java.io.Serializable;
import java.util.Comparator;

//TODO: [rtisma] -- consider storing the WorkflowTypes, MutationTypes and MutationSubTypes enum values instead of string representation. Or atleast keep strings, just create functions to compare the string against the enum
@Slf4j
@Data
public final class PortalMetadata implements Serializable {

  private static final long serialVersionUID = 1484172786L;

  @NonNull
  private final String objectId;

  @NonNull
  private final String fileId;

  @NonNull
  private final String sampleId;

  @NonNull
  private final String donorId;

  @NonNull
  private final String dataType;

  @NonNull
  private final String referenceName;

  @NonNull
  private final String genomeBuild;

  private final long fileSize;

  @NonNull
  private final String fileMd5sum;

  @NonNull
  private final PortalFilename portalFilename;

  public static PortalMetadata buildPortalMetadata(@NonNull final ObjectNode objectNode){
    val objectId = PortalFiles.getObjectId(objectNode);
    val fileId = PortalFiles.getFileId(objectNode);
    val sampleId = PortalFiles.getSampleId(objectNode);
    val donorId = PortalFiles.getDonorId(objectNode);
    val dataType = PortalFiles.getDataType(objectNode);
    val referenceName = PortalFiles.getReferenceName(objectNode);
    val genomeBuild = PortalFiles.getGenomeBuild(objectNode);
    val portalFilename = PortalFiles.getPortalFilename(objectNode);
    val fileSize = PortalFiles.getFileSize(objectNode);
    val fileMd5sum = PortalFiles.getFileMD5sum(objectNode);
    return new PortalMetadata(objectId, fileId, sampleId, donorId, dataType, referenceName, genomeBuild, fileSize,
        fileMd5sum,
        portalFilename);
  }

  public double getFileSizeMb() {
    return (double) getFileSize() / (1024 * 1024);
  }

  public static class FileSizeComparator implements Comparator<PortalMetadata> {

    @Override
    public int compare(PortalMetadata f1, PortalMetadata f2) {
        return Long.compare(f1.getFileSize(), f2.getFileSize());
    }

  }

  public static class FilenameComparator implements Comparator<PortalMetadata> {

    @Override
    public int compare(PortalMetadata f1, PortalMetadata f2) {
      return f1.getPortalFilename().getFilename().compareTo(f2.getPortalFilename().getFilename());
    }

  }

}
