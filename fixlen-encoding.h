// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_FIXLEN_ENCODING_H
#define IMPALA_FIXLEN_ENCODING_H

#include <immintrin.h>

namespace impala {

class FixLenDecoder {
 public:
  FixLenDecoder(const char* buffer, int buffer_len, int bit_width)
    : buffer_(buffer),
      buffer_len_(buffer_len),
      start_index_(0),
      cache_counter_(0) {
    if (bit_width == 16) {
      shufflemask1 = _mm256_setr_epi32(0x80800100, 0x80800201, 0x80800302, 0x80800403,
          0x80800100, 0x80800201, 0x80800302, 0x80800403);
      shufflemask2 = _mm256_setr_epi32(0x80800504, 0x80800605, 0x80800706, 0x80800807,
          0x80800504, 0x80800605, 0x80800706, 0x80800807);
      shufflemask3 = _mm256_setr_epi32(0x80800a09, 0x80800b0a, 0x80800c0b, 0x80800d0c,
          0x80800a09, 0x80800b0a, 0x80800c0b, 0x80800d0c);
      shufflemask4 = _mm256_setr_epi32(0x80800e0d, 0x80800f0e, 0x80808080, 0x80808080,
          0x80800e0d, 0x80800f0e, 0x80808080, 0x80808080);
      shufflemask5 = _mm256_setr_epi32(0x80808080, 0x80808080, 0x80800e0d, 0x80800f0e,
          0x80808080, 0x80808080, 0x80800e0d, 0x80800f0e);
      shiftmask1 = _mm256_set_epi32(3, 2, 1, 0, 3, 2, 1, 0);
      shiftmask2 = _mm256_set_epi32(7, 6, 5, 4, 7, 6, 5, 4);
      clearmask = _mm256_set1_epi32(0x000001ff);
      shiftmask3 = _mm256_set_epi32(5, 4, 5, 4, 5, 4, 5, 4);
    }

    _mm_prefetch((const char*)buffer_, _MM_HINT_T0);
  }

  FixLenDecoder() {}

  // Returns the number of unpacked data.
  int unpack16(int* val) {
    if (UNLIKELY(start_index_ - cache_counter_ > L3_CACHE)) {
      _mm_prefetch((const char*)(buffer_ + start_index_), _MM_HINT_T0);
      cache_counter_ = start_index_;
    }
    if (UNLIKELY(buffer_len_ - start_index_ < 64)) {
      if (UNLIKELY(buffer_len_ - start_index_ < 32)) {
        //TODO
      } else {
        //TODO
        return 8;
      }
    } else {
      __m256i inreg1, inreg2, midreg, ret1, ret2, outreg1, outreg2, tail1, tail2;
      inreg1 = _mm256_loadu_si256((__m256i const*)(buffle_ + start_index_));
      midreg = _mm256_shuffle_epi8(inreg1, shufflemask1);
      midreg = _mm256_srlv_epi32(midreg, shiftmask1);
      ret = _mm256_and_si256(midreg, clearmask);
      _mm256i_storeu_si256((__m256i*)val, ret);
      midreg = _mm256_shuffle_epi8(inreg1, shufflemask2);
      midreg = _mm256_srlv_epi32(midreg, shiftmask2);
      ret = _mm256_and_si256(midreg, clearmask);
      _mm256i_storeu_si256((__m256i*)(val + 8), ret);
      midreg = _mm256_shuffle_epi8(inreg1, shufflemask3);
      midreg = _mm256_srlv_epi32(midreg, shiftmask1);
      ret = _mm256_and_si256(midreg, clearmask);
      _mm256i_storeu_si256((__m256i*)(val + 16), ret);

      inreg2 = _mm256_loadu_si256((__m256i const*)(buffer_ + start_index_ + 32));
      midreg = _mm256_shuffle_epi8(inreg2, shufflemask1);
      midreg = _mm256_srlv_epi32(midreg, shiftmask1);
      ret = _mm256_and_si256(midreg, clearmask);
      _mm256i_storeu_si256((__m256i*)(val + 24), ret);
      midreg = _mm256_shuffle_epi8(inreg2, shufflemask2);
      midreg = _mm256_srlv_epi32(midreg, shiftmask2);
      ret = _mm256_and_si256(midreg, clearmask);
      _mm256i_storeu_si256((__m256i*)(val + 32), ret);
      midreg = _mm256_shuffle_epi8(inreg2, shufflemask3);
      midreg = _mm256_srlv_epi32(midreg, shiftmask1);
      ret = _mm256_and_si256(midreg, clearmask);
      _mm256i_storeu_si256((__m256i*)(val + 40), ret);

      tail1 = _mm256_shuffle_epi8(inreg1, shufflemask4);
      tail2 = _mm256_shuffle_epi8(inreg2, shufflemask5);
      midreg = _mm256_or_si256(tail1, tail2);
      midreg = _mm256_srlv_epi32(midreg, shiftmask3);
      ret = _mm256_and_si256(midreg, clearmask);
      _mm256i_storeu_si256((__m256i*)(val + 48), ret);
      start_index_ += 64;
      return 56;
    }
  }

 private:
  const char* buffer_;
  int buffer_len_;
  int start_index_;
  __m256i shufflemask1;
  __m256i shufflemask2;
  __m256i shufflemask3;
  __m256i shufflemask4;
  __m256i shufflemask5;
  __m256i shiftmask1;
  __m256i shiftmask2;
  __m256i clearmask;
  __m256i shiftmask3;
};

class FixLenEncoder {
 public:
  FixLenEncoder(const char* buffer, int buffer_len, int bit_width)
    : buffer_(buffer),
      buffer_len_(buffer_len),
      start_index_(0) {
    if (bit_width == 16) {
      shiftmask1 = _mm256_set_epi32(23*4, 23*3, 23*2, 23, 23*4, 23*3, 23*2, 23);
      shiftmask2 = _mm256_set1_epi32(36);
      shiftmask3 = _mm256_set1_epi32(36*2);

      shiftmask1 = _mm256_set_epi32(3, 2, 1, 0, 3, 2, 1, 0);
      shiftmask2 = _mm256_set_epi32(7, 6, 5, 4, 7, 6, 5, 4);
      clearmask = _mm256_set1_epi32(0x000001ff);

      clearmask1 = _mm256_set_epi32(0x0, 0x0, 0x0, 0x0003ffff);
      clearmask2 = _mm256_set_epi32(0x0, 0x0, 0x0, 0x0ffc0000);
      shiftmask4 = _mm256_set1_epi32(236);
      shiftmask5 = _mm256_set1_epi32(218);
    }
  }

  bool pack16(int* val, int len) {
    if (buffer_len_ - start_index_ < len * 1.2) return false;
    while (len > 0) {
      if (UNLIKELY(len < 56)) {
        if (UNLIKELY(len < 8)) {
          while (len > 0) {
           //TODO
          }
          break;
        } else {
          //TODO
        }
      } else {
        while(len > 56) {
          __m256i inreg1, inreg2, midreg, ret1, ret2, outreg1, outreg2, tail;
          inreg1 = _mm256_loadu_si256((__m256i const*)(val));
          outreg1 = _mm256_sllv_epi32(inreg1, shiftmask1);
          inreg1 = _mm256_loadu_si256((__m256i const*)(val + 8));
          midreg = _mm256_sllv_epi32(inreg1, shiftmask1);
          midreg = _mm256_srlv_epi32(midreg, shiftmask2);
          outreg1 = _mm256_or_si256(outreg1, midreg);
          inreg1 = _mm256_loadu_si256((__m256i const*)(val + 16));
          midreg = _mm256_sllv_epi32(inreg1, shiftmask1);
          midreg = _mm256_srlv_epi32(midreg, shiftmask3);
          outreg1 = _mm256_or_si256(outreg1, midreg);

          inreg2 = _mm256_loadu_si256((__m256i const*)(val + 24));
          outreg2 = _mm256_sllv_epi32(inreg2, shiftmask1);
          inreg2 = _mm256_loadu_si256((__m256i const*)(val + 32));
          midreg = _mm256_sllv_epi32(inreg2, shiftmask1);
          midreg = _mm256_srlv_epi32(midreg, shiftmask2);
          outreg2 = _mm256_or_si256(outreg2, midreg);
          inreg2 = _mm256_loadu_si256((__m256i const*)(val + 40));
          midreg = _mm256_sllv_epi32(inreg2, shiftmask1);
          midreg = _mm256_srlv_epi32(midreg, shiftmask3);
          outreg2 = _mm256_or_si256(outreg2, midreg);

          tail = _mm256_loadu_si256((__m256i const*)(val + 48));
          tail = _mm256_sllv_epi32(tail, shiftmask1)
          midreg = _mm256_and_si256(tail, clearmask1);
          midreg = _mm256_sllv_epi32(midreg, shiftmask3);
          outerg1 = _mm256_or_si256(outreg1, midreg);

          midreg = _mm256_and_si256(tail, clearmask2);
          midreg = _mm256_sllv_epi32(midreg, shiftmask4);
          outerg2 = _mm256_or_si256(outreg2, midreg);

          _mm256i_storeu_si256((__m256i*)(buffer_ + start_index_), outreg1);
          _mm256i_storeu_si256((__m256i*)(buffer_ + start_index_ + 32), outreg2);
          len -= 56;
          val += 56;
          start_index_ -= 64;
        }
      }
    }
    return true;
  }

 private:
  const char* buffer_;
  int buffer_len_;
  int start_index_;

  __m256i shiftmask1;
  __m256i shiftmask2;
  __m256i shiftmask3;
  __m256i shiftmask1;
  __m256i shiftmask2;
  __m256i shiftmask4;
  __m256i shiftmask5;
  __m256i clearmask;
  __m256i clearmask1;
  __m256i clearmask2;
};
}
#endif
