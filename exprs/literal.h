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


#ifndef IMPALA_EXPRS_LITERAL_H_
#define IMPALA_EXPRS_LITERAL_H_

#include <string>

namespace impala {


class Literal : public Expr {
 public:

  Literal(int value);

 protected:

  virtual std::string DebugString() const;

 private:

  int value_;
};

}

#endif
