#pragma once

#include "facade/reply_capture.h"

namespace facade {

std::string FormatToJson(facade::CapturingReplyBuilder::Payload&& value);

};
