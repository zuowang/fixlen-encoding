// Copyright 2015 Cloudera Inc.
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
#include <stdint.h>

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

#define L3_CACHE 6144*1024

namespace impala {

template <int bit_width>
class FixLenDecoder {
 public:
  FixLenDecoder(const char* buffer, int buffer_len, int num_val);

  FixLenDecoder();

  int Unpack(int* val);

 private:
  const char* buffer_;
  int buffer_len_;
  int start_index_;
  int num_val_;
  int cache_counter_;
};

//
// Encoded data are loaded into AVX integer vector register. It's 256
// bits and hold 28 integers.
// 0               64             128 0              64             128
// |  7 9-bits int  |  7 9-bits int  |  7 9-bits int  |  7 9-bits int  |
//
//
// Below is how integer a, b, c, d, e, f, g decoded. Each of them takes
// up 9 bits.
// 0                               32                                64
// |dddddcccccccccbbbbbbbbbaaaaaaaaa|0gggggggggfffffffffeeeeeeeeedddd|
//
// Integer a, b, c, e, f, g are decoded by shifting to be hold in a 32
// bit integer and then clearing the unused bits.
// Integer d is decoded by shifting to be hold in a 64 bit integer and
// then combining with another AVX vector register.
// 0                               32                                64
// |00000000000000000000000zzzzzzzzz|00000000000000000000000ddddddddd|
//
//
template <>
class FixLenDecoder<9> {
 public:
  FixLenDecoder(const char* buffer, int buffer_len, int num_val)
    : buffer_(buffer),
      buffer_len_(buffer_len),
      start_index_(0),
      num_val_(num_val)
      cache_counter_(0) {
      clearmask1 = _mm256_set1_epi32(0x1ff);
      clearmask2 = _mm256_set1_epi64x(0xff8000000);
      shiftmask1 = _mm256_setr_epi32(0, 4, 0 ,4, 0, 4, 0, 4);
      clear128mask1 = _mm_set1_epi32(0x1ff);
    _mm_prefetch((const char*)buffer_, _MM_HINT_T0);
  }

  FixLenDecoder() {}

  // Returns the number of unpacked data.
  int Unpack(int* val) {
    if (UNLIKELY(start_index_ - cache_counter_ > L3_CACHE)) {
      _mm_prefetch((const char*)(buffer_ + start_index_), _MM_HINT_T0);
      cache_counter_ = start_index_;
    }
    if (UNLIKELY(buffer_len_ - start_index_ < 64)) {
      return 0;
    } else {
      __m256i in1, in2, a1, a2, tmp, tail;
      in1 = _mm256_loadu_si256((__m256i const*)(buffer_ + start_index_));//3
      a1 = _mm256_srlv_epi32(in1, shiftmask1);//2
      tmp = _mm256_and_si256(a1, clearmask1);//1
      _mm256_storeu_si256((__m256i*)val, tmp);//4
      tmp = _mm256_srli_epi32(a1, 9);//1
      tmp = _mm256_and_si256(tmp, clearmask1);//1
      _mm256_storeu_si256((__m256i*)(val + 8), tmp);//4
      tmp = _mm256_srli_epi32(a1, 18);//1
      tmp = _mm256_and_si256(tmp, clearmask1);//1
      _mm256_storeu_si256((__m256i*)(val + 16), tmp);//4

      in2 = _mm256_loadu_si256((__m256i const*)(buffer_ + start_index_ + 32));//3
      a2 = _mm256_srlv_epi32(in2, shiftmask1);//2
      tmp = _mm256_and_si256(a2, clearmask1);//1
      _mm256_storeu_si256((__m256i*)(val + 24), tmp);//4
      tmp = _mm256_srli_epi32(a2, 9);//1
      tmp = _mm256_and_si256(tmp, clearmask1);//1
      _mm256_storeu_si256((__m256i*)(val + 32), tmp);//4
      tmp = _mm256_srli_epi32(a2, 18);//1
      tmp = _mm256_and_si256(tmp, clearmask1);//1
      _mm256_storeu_si256((__m256i*)(val + 40), tmp);//4

      tmp = _mm256_and_si256(in1, clearmask2);//1
      tail = _mm256_slli_epi64(tmp, 5);//1
      tmp = _mm256_and_si256(in2, clearmask2);//1
      tmp = _mm256_srli_epi64(tmp, 27);//1
      tail = _mm256_or_si256(tail, tmp);//1
      _mm256_storeu_si256((__m256i*)(val + 48), tail);//4

      start_index_ += 64;
      num_val -= 56;
      if (UNLIKELY(num_val < 0)) return num_val + 56;
      return 56;
    }
  }

  __m256i Top() {
    return _mm256_loadu_si256((__m256i const*)(buffer_ + start_index_));//3
  }

 private:
  const char* buffer_;
  int buffer_len_;
  int start_index_;
  int cache_counter_;
  __m256i clearmask1;
  __m256i clearmask2;
  __m256i clearmask3;
  __m256i shiftmask1;
  __m128i clear128mask1;
};

template <int bit_width>
class FixLenEncoder {
 public:
  FixLenEncoder(char* buffer, int buffer_len);

  bool Pack(int* val, int len);

  int len();

 private:
  char* buffer_;
  int buffer_len_;
  int start_index_;
  __m256i shiftmask1;
  __m256i shiftmask2;
  __m256i shiftmask3;
  __m256i shiftmask4;
  __m256i shiftmask5;
  __m256i clearmask1;
  __m256i clearmask2;
};

// Below is how integer a, b, c, d, e, f, g encoded. Each of them takes
// up 9 bits.
// 0                               32                                64
// |dddddcccccccccbbbbbbbbbaaaaaaaaa| gggggggggfffffffffeeeeeeeeedddd|
//
// An AVX vector register is 256 bits. It can hold 28 integers. However,
// we load 8 integers into an AVX vector register at a time. Therefore,
// we load 7 times, and encode 56 integers into 2 AVX vector registers
// at each call to Pack().
//
// When they are not 56 integers left at the end of encoding, padding
// the input data buffer to 56 integers.
//
template <>
class FixLenEncoder<9> {
 public:
  FixLenEncoder(char* buffer, int buffer_len)
    : buffer_(buffer),
      buffer_len_(buffer_len),
      start_index_(0),
      num_val(0) {
    clearmask1 = _mm256_setr_epi32(0x0, 0x1ff, 0x0, 0x1ff, 0x0, 0x1ff, 0x0, 0x1ff);
    clearmask2 = _mm256_setr_epi32(0x1ff, 0x0, 0x1ff, 0x0, 0x1ff, 0x0, 0x1ff, 0x0);
    shiftmask1 = _mm256_setr_epi32(0, 4, 0 ,4, 0, 4, 0, 4);
  }

  bool Pack(int* val, int len) {
    while (len > 0) {
      if (UNLIKELY(len < 64)) {
        return false;
      } else {
        while(len >= 64) {
          __m256i out1, out2, tmp, tail;
          out1 = _mm256_loadu_si256((__m256i const*)(val));//3
          tmp = _mm256_loadu_si256((__m256i const*)(val + 8));//3
          tmp = _mm256_slli_epi32(tmp, 9);//1
          out1 = _mm256_or_si256(out1, tmp);//1
          tmp = _mm256_loadu_si256((__m256i const*)(val + 16));//3
          tmp = _mm256_slli_epi32(tmp, 18);//1
          out1 = _mm256_or_si256(out1, tmp);//1
          out1 = _mm256_sllv_epi32(out1, shiftmask1);//2

          out2 = _mm256_loadu_si256((__m256i const*)(val + 24));//3
          tmp = _mm256_loadu_si256((__m256i const*)(val + 32));//3
          tmp = _mm256_slli_epi32(tmp, 9);//1
          out2 = _mm256_or_si256(out2, tmp);//1
          tmp = _mm256_loadu_si256((__m256i const*)(val + 40));//3
          tmp = _mm256_slli_epi32(tmp, 18);//1
          out2 = _mm256_or_si256(out2, tmp);//1
          out2 = _mm256_sllv_epi32(out2, shiftmask1);//2

          tail = _mm256_loadu_si256((__m256i const*)(val + 48));//3
          tmp = _mm256_and_si256(tail, clearmask1);//1
          tmp = _mm256_srli_epi64(tmp, 5);//1
          out1 = _mm256_or_si256(out1, tmp);//1
          tmp = _mm256_and_si256(tail, clearmask2);//1
          tmp = _mm256_slli_epi64(tmp, 27);//1
          out2 = _mm256_or_si256(out2, tmp);//1

          _mm256_storeu_si256((__m256i*)(buffer_ + start_index_), out1);//4
          _mm256_storeu_si256((__m256i*)(buffer_ + start_index_ + 32), out2);//4
          len -= 56;
          val += 56;
          num_val += 56;
          start_index_ += 64;
        }
      }
    }
    return true;
  }

  int len() { return start_index_;}

 private:
  char* buffer_;
  int buffer_len_;
  int start_index_;
  int num_val;

  __m256i shiftmask1;
  __m256i clearmask1;
  __m256i clearmask2;
};

}

#endif
