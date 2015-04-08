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


#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <string>
#include <vector>

namespace impala {

class Expr;

// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  virtual ~Expr();

  void AddChild(Expr* expr) { children_.push_back(expr); }
  Expr* GetChild(int i) const { return children_[i]; }
  int GetNumChildren() const { return children_.size(); }

  bool is_slotref() const { return is_slotref_; }

  const std::vector<Expr*>& children() const { return children_; }

  // Returns the slots that are referenced by this expr tree in 'slot_ids'.
  // Returns the number of slots added to the vector
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;

  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<Expr*>& exprs);

 protected:

  // recognize if this node is a slotref in order to speed up GetValue()
  const bool is_slotref_;
  std::vector<Expr*> children_;

  // Simple debug string that provides no expr subclass-specific information
  std::string DebugString(const std::string& expr_name) const {
    std::stringstream out;
    out << expr_name << "(" << Expr::DebugString() << ")";
    return out.str();
  }

 private:

};

}

#endif
