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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.metadata.FileContext;
import org.icgc.dcc.pcawg.client.model.metadata.FileMetaData;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@RequiredArgsConstructor(access =  PRIVATE)
@Value
public class PortalFileDownloader implements Iterable<FileContext> {

  public static final PortalFileDownloader newPortalFileDownloader(final PortalNew portal, final Storage storage){
    return new PortalFileDownloader(portal, storage);
  }

  @NonNull
  private final PortalNew portal;

  @NonNull
  private final Storage storage;

  @NonFinal
  private List<FileMetaData> fileMetaDatas = null;

  @Override
  public Iterator<FileContext> iterator() {
    val iterator = getFileMetaDatas().iterator();
    return new Iterator<FileContext>(){

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public FileContext next() {
        return createFileContext(iterator.next());
      }
    };
  }

  public Stream<FileContext> stream() {
    return getFileMetaDatas().stream()
        .map(this::createFileContext);
  }

  //Lazy loading
  private List<FileMetaData> getFileMetaDatas(){
    if (fileMetaDatas == null){
      val fileMetas = portal.getFileMetas();
      this.fileMetaDatas = fileMetas.stream()
          .map(FileMetaData::buildFileMetaData)
          .collect(toImmutableList());
    }
    return fileMetaDatas;
  }

  private  FileContext createFileContext(FileMetaData fileMetaData){
    return FileContext.builder()
        .file(storage.downloadFile(fileMetaData))
        .fileMetaData(fileMetaData)
        .build();
  }

}
