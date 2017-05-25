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

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;

@RequiredArgsConstructor
public enum DataTypes {
  INDEL("indel"),
  SNV_MNV("snv_mnv"),
  CNV("cnv"),
  SV("sv"),
  UNKNOWN("unknown");

  private static final String CLASS_NAME = DataTypes.class.getCanonicalName();

  @NonNull
  @Getter
  private final String name;

  public boolean equals(@NonNull final String name) {
    return this.getName().equals(name);
  }

  public boolean isAtBeginningOf(@NonNull final String name) {
    return name.matches("^" + this.getName() + ".*");
  }

  public boolean isIn(@NonNull final String name) {
    return name.contains(this.getName());
  }

  private static DataTypes parse(String name, Predicate<DataTypes> predicate){
    for (val v : values()){
      if (predicate.test(v)){
        return v;
      }
    }
    return DataTypes.UNKNOWN;
  }

  public static DataTypes parseStartsWith(String name, boolean check){
    val dataType = parse(name, d -> d.isAtBeginningOf(name) );
    if (check){
      checkState(dataType != DataTypes.UNKNOWN,
          "%s does not contain a dataType that starts with [%s]", CLASS_NAME, name);
    }
    return dataType;
  }

  public static DataTypes parseMatch(String name, boolean check){
    val dataType = parse(name, d -> d.equals(name) );
    if (check) {
      checkState(dataType != DataTypes.UNKNOWN,
          "The name [%s] does not match any dataType name in %s", name, CLASS_NAME);
    }
    return dataType;
  }

  public static DataTypes parseContains(String name, boolean check){
    val dataType = parse(name, d -> d.isIn(name) );
    if (check) {
      checkState(dataType != DataTypes.UNKNOWN,
          "The name [%s] does exist in any of the dataTypes in %s", name, CLASS_NAME);
    }
    return dataType;
  }

  @Override
  public String toString() {
    return getName();
  }

}
