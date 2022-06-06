// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class StreamFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(StreamFamilyTest, Add) {
  Run({"xadd", "key", "*", "field", "value"});
  Run({"xrange", "key", "-", "+"});
}

}  // namespace dfly