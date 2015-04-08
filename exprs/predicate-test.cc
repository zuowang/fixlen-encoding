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

#include "exprs/predicate.h"
#include <vector>

using namespace std;
using namespace impala;

namespace impala {
  const int COLUMN_DATALEN = 102400 * 64;
  typedef vector<vector<int> > VectorRowBatch; 
  class ColumnReader {
    int8_t* data_;
    int len_;
    FixlenDecoder<9>* dec_;
  }
  
  int main() {
    vector<ColumnReader> cols(6, ColumnReader());

    for (int i = 0; i< 6; ++i) {
      cols[i].len_ = COLUMN_DATALEN;
      cols[i].data_ = new int8_t[COLUMN_DATALEN];
      cols[i].dec_ = new FixLenDecoder<9>(cols[i].data_, cols[i].len_);
    }

    SlotRef slot0(0);
    Operator::EQ op1(slot0, 3);
    SlotRef slot1(1);
    Operator::EQ op2(slot1, 4);
    CompoundPredicate::AndPredicate and1(op1, op2);
    SlotRef slot2(2);
    InPredicate in1(slot2, 5, 6, 8);
    CompoundPredicate::AndPredicate and2(and1, in1);
    SlotRef slot3(3);
    Operator::LE op3(slot3, 11);
    InPredicate in2(slot3, 15, 16, 17);
    CompoundPredicate::OrPredicate or1(op3, in2);
    CompoundPredicate::AndPredicate and3(and2, or1);
    SlotRef slot4(4);
    Operator::GT op4(slot4, 5);
    SlotRef slot5(5);
    Operator::LE op5(slot5, 60);
    CompoundPredicate::AndPredicate and4(op4, op5);
    CompoundPredicate::Orpredicate root_or(and3, and4);
    VectorRowBatch vec_row_batch = root_or.Eval(cols);

  }
} // namespace impala
