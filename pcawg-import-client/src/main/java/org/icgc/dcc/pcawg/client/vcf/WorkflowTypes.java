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
package org.icgc.dcc.pcawg.client.vcf;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@RequiredArgsConstructor
@Getter
public enum WorkflowTypes {

  CONSENSUS("consensus", set(" PCAWG SNV-MNV callers", "PCAWG InDel callers")),
  SANGER("sanger", set("Sanger variant call pipeline")),
  DKFZ_EMBL("dkfz", set("DKFZ/EMBL variant call pipeline")),
  BROAD("broad", set("Broad variant call pipeline")),
  MUSE("muse", set("MUSE variant call pipeline") ),
  SMUFIN("smufin", set("SMUFIN variant call pipeline") ),
  UNKNOWN("unknown", set("UNKNOWN variant call pipeline") );

  private static final boolean DEFAULT_NO_ERRORS_FLAG = false;
  private static Set<String> set(String ... strings){
    return stream(strings).collect(toImmutableSet());
  }

  @NonNull
  private final String name;

  @NonNull
  private final Set<String> portalSoftwareNames;

  public boolean equals(@NonNull final String name) {
    return this.getName().equals(name);
  }

  public boolean isAtBeginningOf(@NonNull final String name) {
    return name.matches("^" + this.getName() + ".*");
  }

  public boolean isIn(@NonNull final String name) {
    return name.contains(this.getName());
  }

  private static WorkflowTypes parse(String name, Predicate<WorkflowTypes> predicate){
    for (val v : values()){
      if (predicate.test(v)){
        return v;
      }
    }
    return WorkflowTypes.UNKNOWN;
  }

  public static WorkflowTypes parseStartsWith(String name, boolean check){
    val workflowType = parse(name, w -> w.isAtBeginningOf(name) );
    if (check){
      checkState(workflowType != WorkflowTypes.UNKNOWN, "%s does not contain a workflowType that starts with [%s]", WorkflowTypes.class.getName(), name);
    }
    return workflowType;
  }

  public static WorkflowTypes parseMatch(String name, boolean check){
    val workflowType = parse(name, w -> w.equals(name) );
    if (check) {
      checkState(workflowType != WorkflowTypes.UNKNOWN, "The name [%s] does not match any workflowType name in %s", name,
          WorkflowTypes.class.getName());
    }
    return workflowType;
  }

  public static WorkflowTypes parseContains(String name, boolean check){
    val workflowType = parse(name, w -> w.isIn(name) );
    if (check) {
      checkState(workflowType != WorkflowTypes.UNKNOWN, "The name [%s] does exist in any of the workflowTypes in %s", name,
          WorkflowTypes.class.getName());
    }
    return workflowType;
  }

  @Override
  public String toString() {
    return this.getName();
  }
}
