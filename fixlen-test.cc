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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <math.h>
#include "time.h"

#include "fixlen-encoding.h"

using namespace std;
using namespace impala;

int main(int argc, char **argv) {
  /*
  {
    int data[64];
    for (int i = 0; i < 56; ++i) {
      data[i] = i;
    }
    char src[256];
    int* psrc = (int*)src;
    int dest[64];
    FixLenEncoder<91> fle(src, 256);
    if (fle.Pack(data, 56)) {
      for (int i = 0; i < fle.len(); i+=4) {
        printf("0x%.8x ", psrc[i]);
      }
      printf("\n");
      FixLenDecoder<91> fld(src, fle.len());
      int ret;
      while((ret = fld.Unpack(dest)) > 0) {
        printf("ret = %d\n", ret);
        for (int i = 0; i < ret; ++i) {
          printf("%d ", dest[i]);
        }
        printf("\n");
      }
    }
  }
  */
  int data[1024*1024];
  int ret;
  int dest[64];
  clock_t start = clock();
  for (int j = 0; j< 10000000; ++j) {
    FixLenDecoder<91> fld((const char*)data, 4*1024*1024);
    while((ret = fld.Unpack(dest)) > 0);
  }
  clock_t end = clock();
  printf("%f\n", (double)(end - start));
  return 0;
}

