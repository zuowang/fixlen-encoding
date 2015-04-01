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

#include "fixlen-encoding.h"

using namespace std;
using namespace impala;

int main(int argc, char **argv) {
  {//len==32
    int16_t data[16] = {0,1,2,3,8,9,10,11,4,5,6,7,12,13,14,15};
    const char* src = (const char*)data;
    int dest[64];
    FixLenDecoder fld(src, 32, 16);
    int ret = fld.unpack16(dest);
    printf("ret = %d\n", ret);
    for (int i = 0; i < 16; i++) {
      printf("%d ", dest[i]);
    }
    printf("\n");
  }
  {//len==48
    int16_t data[24] = {0,1,2,3,8,9,10,11,4,5,6,7,12,13,14,15,16,17,18,19,20,21,22,23};
    const char* src = (const char*)data;
    int dest[64];
    FixLenDecoder fld(src, 48, 16);
    int ret;
    while((ret = fld.unpack16(dest)) > 0) {
      printf("ret = %d\n", ret);
      for (int i = 0; i < ret; i++) {
        printf("%d ", dest[i]);
      }
      printf("\n");
    }
  }
  {//len==50
    int16_t data[25] = {0,1,2,3,8,9,10,11,4,5,6,7,12,13,14,15,16,17,18,19,20,21,22,23,24};
    const char* src = (const char*)data;
    int dest[64];
    FixLenDecoder fld(src, 50, 16);
    int ret;
    while((ret = fld.unpack16(dest)) > 0) {
      printf("ret = %d\n", ret);
      for (int i = 0; i < ret; i++) {
        printf("%d ", dest[i]);
      }
      printf("\n");
    }
  }
  {//len==100
    int16_t data[50] = {0,1,2,3,8,9,10,11,4,5,6,7,12,13,14,15,16,17,18,19,20,21,22,23,
        24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49};
    const char* src = (const char*)data;
    int dest[64];
    FixLenDecoder fld(src, 50, 16);
    int ret;
    while((ret = fld.unpack16(dest)) > 0) {
      printf("ret = %d\n", ret);
      for (int i = 0; i < ret; i++) {
        printf("%d ", dest[i]);
      }
      printf("\n");
    }
  }

  return 0;
}

