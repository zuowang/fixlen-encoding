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

#include <stdio.h>
#include "time.h"

#include "fixlen-encoding.h"

using namespace std;
using namespace impala;

int main(int argc, char **argv) {
  {//functional test
    int data[128];
    int len;
    scanf("%d", &len);
    for (int i = 0; i < len; ++i) {
      data[i] = i;
    }
    char src[512];
    int* psrc = (int*)src;
    int dest[64];

    char tail[32];
    int tail_len = 0;
    FixLenEncoder<9> fle(src, 512, tail, &tail_len);
    if (fle.Pack(data, len)) {
      FixLenDecoder<9> fld(src, fle.len());
      int ret;
      while((ret = fld.Unpack(dest)) > 0) {
        printf("ret = %d\n", ret);
        for (int i = 0; i < ret; ++i) {
          printf("%d ", dest[i]);
        }
        printf("\n");
      }
      printf("tail = %d\n", tail_len / 2);
      for (int i = 0; i < tail_len; i += 2) {
        printf("%d ", *reinterpret_cast<int16_t*>(tail + i));
      }
      printf("\n");
    }
  }


  {//benchmark test
    int data[1024*1024];
    int ret;
    int dest[64];
    clock_t start = clock();
    for (int j = 0; j< 10000000; ++j) {
      FixLenDecoder<9> fld((const char*)data, 4*1024*1024);
      while((ret = fld.Unpack(dest)) > 0);
    }
    clock_t end = clock();
    printf("%f\n", (double)(end - start));
  }

  return 0;
}

