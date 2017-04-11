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

import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_NOT_SUPPORTED_ERROR;

@RequiredArgsConstructor
public enum MutationTypes {
  INSERTION_LTE_200BP("insertion of <=200bp"),
  DELETION_LTE_200BP("deletion of <=200bp"),
  SINGLE_BASE_SUBSTITUTION("single base substitution"),
  MULTIPLE_BASE_SUBSTITUTION("multiple base substitution (>=2bp and <=200bp)"),
  UNKNOWN("unknown");

  @NonNull
  private final String name;

  @Override
  public String toString() {
    return name;
  }

  private static MutationTypes procError(boolean throwException, String message, VariantContext v, PcawgVariantErrors error){
    if (throwException){
      throw new PcawgVariantException(message, v, error);
    } else {
      return UNKNOWN;
    }
  }
  public static MutationTypes resolveVariant(boolean throwException, VariantContext v){
    val ref = VCF.getReferenceAlleleString(v);
    val alt = VCF.getFirstAlternativeAlleleString(v);
    val refLength = getReferenceAlleleLength(v);
    val altLength = getFirstAlternativeAlleleLength(v);
    val altIsOne = altLength ==1;
    val refIsOne = refLength ==1;
    val refStartsWithAlt = ref.startsWith(alt);
    val altStartsWithRef = alt.startsWith(ref);
    val  lengthDifference = refLength - altLength;
    if (lengthDifference < 0 && !altIsOne && !refStartsWithAlt){
      return refIsOne && altStartsWithRef ? INSERTION_LTE_200BP : MULTIPLE_BASE_SUBSTITUTION;
    } else if(lengthDifference == 0 && !refStartsWithAlt && !altStartsWithRef){
      return refIsOne ? SINGLE_BASE_SUBSTITUTION : MULTIPLE_BASE_SUBSTITUTION;
    } else if(lengthDifference > 0 && !refIsOne && !altStartsWithRef ){
      return altIsOne && refStartsWithAlt ? DELETION_LTE_200BP : MULTIPLE_BASE_SUBSTITUTION;
    } else {
      val message = String.format("The mutationType of the variant cannot be resolved: Ref[%s] Alt[%s] --> RefLength-AltLength=%s,  RefLengthIsOne[%s] AltLengthIsOne[%s], RefStartsWithAlt[%s] AltStartsWithRef[%s] ", lengthDifference, ref, alt, refIsOne, altIsOne, refStartsWithAlt, altStartsWithRef);
      if (throwException){
        throw new PcawgVariantException(message, v, MUTATION_TYPE_NOT_SUPPORTED_ERROR);
      } else {
        return UNKNOWN;
      }
    }
  }

}
