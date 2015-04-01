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
#include "stdint.h"
#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#define L3_CACHE 6144*1024

namespace impala {

class FixLenDecoder {
 public:
  FixLenDecoder(const char* buffer, int buffer_len, int bit_width)
    : buffer_(buffer),
      buffer_len_(buffer_len),
      start_index_(0),
      cache_counter_(0) {
    if (bit_width == 16) {
      shufmaskhi = _mm256_setr_epi32(0x80800100, 0x80800302, 0x80800504, 0x80800706,
          0x80800100, 0x80800302, 0x80800504, 0x80800706);
      shufmasklo = _mm256_setr_epi32(0x80800908, 0x80800b0a, 0x80800d0c, 0x80800f0e,
          0x80800908, 0x80800b0a, 0x80800d0c, 0x80800f0e);
      shufmaskhi128 = _mm_setr_epi32(0x80800100, 0x80800302, 0x80800504, 0x80800706);
      shufmasklo128 = _mm_setr_epi32(0x80800908, 0x80800b0a, 0x80800d0c, 0x80800f0e);
    }
    _mm_prefetch(buffer_, _MM_HINT_T0);
  }

  FixLenDecoder() {}

  // Returns the number of unpacked data.
  int unpack16(int* val) {
    if (UNLIKELY(start_index_ - cache_counter_ > L3_CACHE)) {
      _mm_prefetch(buffer_ + start_index_, _MM_HINT_T0);
      cache_counter_ = start_index_;
    }
    if (UNLIKELY(buffer_len_ - start_index_ < 32)) {
      if (UNLIKELY(buffer_len_ - start_index_ < 16)) {
        int* pval = val;
        while (start_index_ < buffer_len_) {
          *pval = *reinterpret_cast<int16_t*>(
              const_cast<char*>(buffer_ + start_index_));
          ++pval;
          start_index_ += 2;
        }
        return pval - val;
      } else {
        __m128i in = _mm_lddqu_si128((__m128i const*)(buffer_ + start_index_));//3
        __m128i tmp = _mm_shuffle_epi8(in, shufmaskhi128);//1
        _mm_storeu_si128((__m128i*)val, tmp);//3
        tmp = _mm_shuffle_epi8(in, shufmasklo128);//1
        _mm_storeu_si128((__m128i*)(val + 4), tmp);//3
        start_index_ += 16;
        return 8;
      }
    } else {
      __m256i in = _mm256_lddqu_si256((__m256i const*)(buffer_ + start_index_));//3
      __m256i tmp = _mm256_shuffle_epi8(in, shufmaskhi);//1
      _mm256_storeu_si256((__m256i*)val, tmp);//4
      tmp = _mm256_shuffle_epi8(in, shufmasklo);//1
      _mm256_storeu_si256((__m256i*)(val + 8), tmp);//4
      start_index_ += 32;
      return 16;
    }
  }

 private:
  const char* buffer_;
  int buffer_len_;
  int start_index_;
  int cache_counter_;
  __m256i shufmaskhi;
  __m256i shufmasklo;
  __m128i shufmaskhi128;
  __m128i shufmasklo128;
};

class FixLenEncoder {
 public:
  FixLenEncoder(char* buffer, int buffer_len, int bit_width)
    : buffer_(buffer),
      buffer_len_(buffer_len),
      start_index_(0) {
    if (bit_width == 16) {
      shufmaskhi = _mm256_setr_epi32(0x05040100, 0x0d0c0908, 0x80808080, 0x80808080,
          0x05040100, 0x0d0c0908, 0x80808080, 0x80808080);
      shufmasklo = _mm256_setr_epi32(0x80808080, 0x80808080, 0x05040100, 0x0d0c0908,
          0x80808080, 0x80808080, 0x05040100, 0x0d0c0908);
      shufmaskhi128 = _mm_setr_epi32(0x05040100, 0x0d0c0908, 0x80808080, 0x80808080);
      shufmasklo128 = _mm_setr_epi32(0x80808080, 0x80808080, 0x05040100, 0x0d0c0908);
    }
  }

  bool pack16(int* val, int len) {
    if (buffer_len_ - start_index_ < len * 2) return false;
    while (len > 0) {
      if (UNLIKELY(len < 16)) {
        if (UNLIKELY(len < 8)) {
          while (len > 0) {
            *reinterpret_cast<int16_t*>(buffer_ + start_index_) = *val;
            --len;
            ++val;
            start_index_ += 2;
          }
          break;
        } else {
          __m128i in = _mm_loadu_si128((__m128i const*)val);//3
          __m128i tmp = _mm_shuffle_epi8(in, shufmaskhi128);//1
          __m128i in2 = _mm_loadu_si128((__m128i const*)(val + 4));//3
          __m128i tmp2 = _mm_shuffle_epi8(in2, shufmasklo128);//1
          tmp = _mm_or_si128(tmp, tmp2);//1
          _mm_storeu_si128((__m128i*)(buffer_ + start_index_), tmp);//3
          len -= 8;
          val += 8;
          start_index_ += 16;
        }
      } else {
        while(len >= 16) {
          __m256i in = _mm256_loadu_si256((__m256i const*)val);//3
          __m256i tmp = _mm256_shuffle_epi8(in, shufmaskhi);//1
          __m256i in2 = _mm256_loadu_si256((__m256i const*)(val + 8));//3
          __m256i tmp2 = _mm256_shuffle_epi8(in2, shufmasklo);//1
          tmp = _mm256_or_si256(tmp, tmp2);//1
          _mm256_storeu_si256((__m256i*)(buffer_ + start_index_), tmp);//4
          len -= 16;
          start_index_ += 32;
          val += 16;
        }
      }
    }
    return true;
  }
 char* buffer() { return buffer_;}
 private:
  char* buffer_;
  int buffer_len_;
  int start_index_;
  __m256i shufmaskhi;
  __m256i shufmasklo;
  __m128i shufmaskhi128;
  __m128i shufmasklo128;
};
}
#endif
