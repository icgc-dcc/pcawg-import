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
package org.icgc.dcc.pcawg.client.config;

import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;

public class ClientProperties {

  private static final String FALSE = "false";

  public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
  public static final String PORTAL_API = "https://dcc.icgc.org";
  public static final boolean USE_HDFS = parseBoolean(getProperty("use_hdfs", FALSE));
  public static final String TOKEN = getProperty("token");
  public static final String HDFS_ADDRESS = getProperty("hdfs_address","localhost");
  public static final String HDFS_PORT = getProperty("hdfs_port","50075");

  public static final boolean STORAGE_PERSIST_MODE = parseBoolean(getProperty("persist_mode", FALSE));
  public static final String STORAGE_OUTPUT_VCF_STORAGE_DIR = "storedVCFs";
  public static final boolean STORAGE_BYPASS_MD5_CHECK = parseBoolean(getProperty("bypass_md5_check", FALSE));

  public static final String UUID2BARCODE_SHEET_TSV_FILENAME = "pc_annotation-tcga_uuid2barcode.tsv";
  public static final boolean UUID2BARCODE_SHEET_HAS_HEADER= true;
  public static final String UUID2BARCODE_SHEET_TSV_URL = "https://raw.githubusercontent.com/ICGC-TCGA-PanCancer/pcawg-operations/develop/lists/"+UUID2BARCODE_SHEET_TSV_FILENAME;

  public static final String SAMPLE_SHEET_TSV_FILENAME = "pcawg_sample_sheet.2016-10-18.tsv";
  public static final boolean SAMPLE_SHEET_HAS_HEADER = true;
  public static final String SAMPLE_SHEET_TSV_URL = "https://raw.githubusercontent.com/ICGC-TCGA-PanCancer/pcawg-operations/develop/lists/sample_sheet/"+SAMPLE_SHEET_TSV_FILENAME;

  public static final String OUTPUT_TSV_DIRECTORY = "/tmp/tsvDir."+System.currentTimeMillis();

  public static final String SSM_P_TSV_FILENAME_PREFIX = "ssm_p";
  public static final String SSM_P_TSV_FILENAME_EXTENSION = "txt";

  public static final String SSM_M_TSV_FILENAME_PREFIX = "ssm_m";
  public static final String SSM_M_TSV_FILENAME_EXTENSION = "txt";

}
