// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// Included by family object files that implement only their respective registration function.
// Self-registration would require updating the build process to fix linking issues.
namespace dfly {

class CommandRegistry;

void RegisterStringFamily(CommandRegistry*);
void RegisterBitopsFamily(CommandRegistry*);
void RegisterGeoFamily(CommandRegistry*);
void RegisterHllFamily(CommandRegistry*);
void RegisterBloomFamily(CommandRegistry*);
void RegisterJsonFamily(CommandRegistry*);

}  // namespace dfly
