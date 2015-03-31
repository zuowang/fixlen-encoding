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
      shufmaskhi = _mm256_setr_epi32(0x80800100, 0x80800302, 0x80800504, 0x80800706,
          0x80800100, 0x80800302, 0x80800504, 0x80800706);
      shufmasklo = _mm256_setr_epi32(0x80800908, 0x80800b0a, 0x8080d0c, 0x80800f0e,
          0x80800908, 0x80800b0a, 0x8080d0c, 0x80800f0e);
      shufmaskhi128 = _mm_setr_spi32(0x80800100, 0x80800302, 0x80800504, 0x80800706);
      shufmasklo128 = _mm_setr_spi32(0x80800100, 0x80800302, 0x80800504, 0x80800706);
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
    if (UNLIKELY(buffer_len_ - start_index_ < 32)) {
      if (UNLIKELY(buffer_len_ - start_index_ < 16)) {
          int* pval = val;
          while (start_index_ < buffer_len_) {
            *pval = *reinterpret_cast<int16_t*>(buffer_ + start_index_);
            ++pval;
            start_index_ += 2;
          }
          return pval - val;
        } else {
          __m128i in = _mm_lddqu_si128((__m128i const*)(buffer_ + start_index_));//3
          __m128i tmp = _mm_shuffle_epi8(in, shufflemask1);//1
          _mm_storeu_si128((__m128i*)val, tmp);//3
          tmp = _mm_shuffle_epi8(in, shufflemask1);//1
          _mm_storeu_si128((__m128i*)(val + 4), tmp);//3
          start_index_ += 16;
          return 8;
        }
      } else {
        __m256i in = _mm256_lddqu_si256((__m256i const*)(buffer_ + start_index_));//3
        __m256i tmp = _mm256_shuffle_epi8(in, shufflemask1);//1
        _mm256_storeu_si256((__m256i*)val, midreg);//4
        tmp = _mm256_shuffle_epi8(in, shufflemask2);//1
        _mm256_storeu_si256((__m256i*)(val + 8), midreg);//4
        start_index_ += 32;
        return 16;
      }
    }

 private:
  const char* buffer_;
  int buffer_len_;
  int start_index_;
  __m256i shufmaskhi;
  __m256i shufmasklo;
  __m128i shufmaskhi128;
  __m128i shufmasklo128;
};

class FixLenEncoder {
 public:
  FixLenEncoder(const char* buffer, int buffer_len, int bit_width)
    : buffer_(buffer),
      buffer_len_(buffer_len),
      start_index_(0) {
    if (bit_width == 16) {
      slmask = _mm256_set_epi32(64, 48, 32, 16, 64, 48, 32, 16);
      srmask = _mm256_set_epi32(48, 32, 16, 0, 48, 32, 16, 0);
      slmask128 = _mm_set_epi32(32, 24, 16, 8, 32, 24, 16, 8);
      srmask128 = _mm_set_epi32(24, 16, 8, 0, 24, 16, 8, 0);
    }
  }

  bool pack16(int* val, int len) {
    if (buffer_len_ - start_index_ < len * 2) return false;
    while (len > 0) {
      if (UNLIKELY(len < 16)) {
        if (UNLIKELY(len < 8)) {
          while (len > 0) {
            *interpret_cast<int16_t*>(buffer + start_index_) = *val;
            --len;
            ++val;
            start_index_ += 2;
          }
          break;
        } else {
          __m128i in = _mm_loadu_si128((__m128i const*)val);//3
          __m128i tmp = _mm_sllv_epi32(in, slmask128);//2
          __m128i in2 = _mm_loadu_si128((__m128i const*)(val + 4));
          __m256i tmp2 = _mm_srlv_epi32(in2, srmask128);//2
          tmp = _mm_or_epi32(tmp, tmp2);//1
          _mm_storeu_si128((__m128i*)(buffer + start_index_), tmp);//3
          len -= 8;
          val += 8;
          start_index_ += 16;
        }
      } else {
        while(len > 16) {
          __m256i in = _mm256_loadu_si256((__m256i const*)val);//3
          __m256i tmp = _mm256_sllv_epi32(in, slmask);//2
          __m256i in2 = _mm256_loadu_si256((__m256i const*)(val + 8));//3
          __m256i tmp2 = _mm256_srlv_epi32(in2, srmask);//2
          tmp = _mm256_or_epi32(tmp, tmp2);//1
          _mm256_storeu_si256((__m256i*)(buffer + start_index_), tmp);//4
          len -= 16;
          start_index_ += 32;
          val += 16;
        }
      }
    }
    return true;
  }

 private:
  const char* buffer_;
  int buffer_len_;
  int start_index_;
  __m256i slmask;
  __m256i srmask;
  __m128i slmask128;
  __m128i srmask128;
};
}
#endif
