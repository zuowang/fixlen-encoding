#include <immintrin.h>
#include "stdio.h"
#include "stdlib.h"
#include "time.h"
#include "stdint.h"
const int64_t LOOP = 100000000;
int main() {
//int src[8] = {0x200c0401, 0x01c0c050, 0x00000004, 0x0c0d0e0f, 0x10111213,
//    0x14151617, 0x18191a1b, 0x1c1d1e1f};
//int16_t data[16] = {0,1,2,3,8,9,10,11,4,5,6,7,12,13,14,15};
//int8_t* src = (int8_t*)&data;
int8_t* src = new int8_t[32*LOOP];
int* dest = new int[16*LOOP];
//int   dest[16];
__m256i inreg, midreg;
__m256i shufflemask1 = _mm256_setr_epi32(0x80800100, 0x80800302, 0x80800504, 0x80800706,
    0x80800100, 0x80800302, 0x80800504, 0x80800706);
__m256i shufflemask2 = _mm256_setr_epi32(0x80800908, 0x80800b0a, 0x8080d0c, 0x80800f0e,
    0x80800908, 0x80800b0a, 0x8080d0c, 0x80800f0e);
clock_t start = clock();
for (int j = 0; j < 1000; ++j) {
for (int i = 0, k = 0; i < LOOP - k; ++i) {
  if (i == 19000) {
  _mm_prefetch((const char*)(src + k), _MM_HINT_T0);
  k += i;
  i = 0;
  }
  inreg = _mm256_loadu_si256((__m256i const*)(src + i * 32));//6
  //0x00010000 0x00030002 0x00050004 0x00070006 0x00090008 0x000b000a 0x000d000c 0x000f000e
  midreg = _mm256_shuffle_epi8(inreg, shufflemask1);//1
  _mm256_storeu_si256((__m256i*)(dest + i * 16), midreg);//5
  midreg = _mm256_shuffle_epi8(inreg, shufflemask2);//1
  _mm256_storeu_si256((__m256i*)(dest + 8 + i * 16), midreg);//5
}
}
clock_t end = clock();
printf("%f 0x%.8x 0x%.8x 0x%.8x 0x%.8x 0x%.8x 0x%.8x 0x%.8x 0x%.8x\n",
    (double)(end - start), dest[0], dest[1], dest[2], dest[3], dest[4],
    dest[5], dest[6], dest[7]);
return 0;
}

