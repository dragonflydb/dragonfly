// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace dfly {

extern const char kGitTag[];
extern const char kGitSha[];
extern const char kGitClean[];
extern const char kBuildTime[];

const char* GetVersion();

}  // namespace dfly
