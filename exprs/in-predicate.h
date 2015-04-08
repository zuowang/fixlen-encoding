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


#ifndef IMPALA_EXPRS_IN_PREDICATE_H_
#define IMPALA_EXPRS_IN_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"

namespace impala {

class InPredicate : public Predicate {
 public:

 private:

  template<typename T, typename SetType, bool not_in, Strategy strategy>
  static inline impala_udf::BooleanVal TemplatedIn(
      impala_udf::FunctionContext* context, const T& val, int num_args, const T* args);

  // Initializes an SetLookupState in ctx.
  template<typename T, typename SetType>
  static void SetLookupPrepare(
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

  template<typename SetType>
  static void SetLookupClose(
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

  // Looks up v in state->val_set.
  template<typename T, typename SetType>
  static BooleanVal SetLookup(SetLookupState<SetType>* state, const T& v);

  // Iterates through each vararg looking for val. 'type' is the type of 'val' and 'args'.
  template<typename T>
  static BooleanVal Iterate(
      const FunctionContext::TypeDesc* type, const T& val, int num_args, const T* args);
};

}

#endif
